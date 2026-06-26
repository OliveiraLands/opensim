using System;
using System.IO;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Data;
using System.Data.SQLite;
using System.Text;
using System.Timers;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using OpenMetaverse;
using log4net;
using System.Reflection;
using OpenSim.Data;
using System.IO.MemoryMappedFiles;

namespace OpenSim.Services.AdvancedAssetService
{
    public class PackFileIndexEntry { public string Hash; public long Offset; public int Length; public int PackFileID; }
    public class AssetMetadataRecord { public string UUID; public string Hash; public sbyte Type; public string Name; public long Created; public bool Synced; }
    
    public class CachedAssetInfo
    {
        public string Hash;
        public sbyte Type;
        public string Name;
        public long Created;
        public int PackFileID;
        public long Offset;
        public int Length;
    }
    
    public class AssetWriteOp
    {
        public string UUID;
        public byte[] Data;
        public sbyte Type;
        public string Name;
        public long Created;
        public TaskCompletionSource<string> Tcs;
    }

    public class PendingUpdate
    {
        public Action<SQLiteCommand> Action;
        public Action PostCommitAction;
        public Action OnFailureAction;
    }

    public class PackFileManager : IDisposable
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private const uint MAGIC_NUMBER = 0x21534141; // "AAS!"
        private const uint MAGIC_NUMBER_LINK = 0x214B4C41; // "ALK!" (Asset Link)
        private const ushort RECORD_VERSION = 2; // Version 2 includes Creation Date

        private string m_BasePath;
        private string m_IndexFile;
        private SQLiteConnection m_Connection;
        private int m_CurrentPackID = 0;
        private long m_MaxPackSize = 512 * 1024 * 1024;
        private object m_Lock = new object();
        private HashSet<string> m_SuspiciousCache = null;
        private bool m_Disposed = false;

        public int CurrentPackID
        {
            get { lock (m_Lock) { return m_CurrentPackID; } }
        }
        
        private Timer m_BatchTimer;
        private ConcurrentQueue<PendingUpdate> m_PendingUpdates = new ConcurrentQueue<PendingUpdate>();
        private BlockingCollection<AssetWriteOp> m_WriteQueue = new BlockingCollection<AssetWriteOp>(5000);
        private ConcurrentDictionary<string, AssetWriteOp> m_PendingWritesCache = new ConcurrentDictionary<string, AssetWriteOp>(StringComparer.OrdinalIgnoreCase);

        private ConcurrentDictionary<string, PackFileIndexEntry> m_InFlightHashes = new ConcurrentDictionary<string, PackFileIndexEntry>();
        private Task m_WriteTask;
        private object m_StoreLock = new object();
        private readonly ConcurrentDictionary<string, CachedAssetInfo> m_L1Cache = new ConcurrentDictionary<string, CachedAssetInfo>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<int, MemoryMappedFile> m_MappedPacks = new ConcurrentDictionary<int, MemoryMappedFile>();

        public PackFileManager(string basePath)
        {
            m_BasePath = basePath;
            if (!Directory.Exists(m_BasePath)) Directory.CreateDirectory(m_BasePath);
            m_IndexFile = Path.Combine(m_BasePath, "index.db");
            InitializeDatabase();

            m_BatchTimer = new Timer(2000);
            m_BatchTimer.AutoReset = true;
            m_BatchTimer.Elapsed += (s, e) => FlushBatch();
            m_BatchTimer.Start();

            m_WriteTask = Task.Factory.StartNew(ProcessWriteQueue, TaskCreationOptions.LongRunning);

            // Run link migration asynchronously in the background so it doesn't block startup
            Task.Run(() =>
            {
                try
                {
                    MigrateDuplicateLinks();
                }
                catch (Exception ex)
                {
                    m_log.Error("[AAS Migration]: Background migration failed: " + ex.Message);
                }
            });
        }

        public void ClearSuspiciousStatus(string uuid)
        {
            if (m_SuspiciousCache == null)
            {
                lock (m_Lock)
                {
                    if (m_SuspiciousCache == null)
                    {
                        m_SuspiciousCache = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        using (var cmd = m_Connection.CreateCommand())
                        {
                            cmd.CommandText = "SELECT uuid FROM suspicious_assets";
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    m_SuspiciousCache.Add(reader.GetString(0));
                                }
                            }
                        }
                    }
                }
            }

            if (m_SuspiciousCache.Count > 0 && m_SuspiciousCache.Contains(uuid))
            {
                lock (m_Lock)
                {
                    m_SuspiciousCache.Remove(uuid);
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "DELETE FROM suspicious_assets WHERE uuid = ?";
                        cmd.Parameters.AddWithValue(null, uuid);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public void SetSuspiciousAssets(IEnumerable<string> uuids)
        {
            lock (m_Lock)
            {
                using (var trans = m_Connection.BeginTransaction())
                {
                    try
                    {
                        using (var cmd = m_Connection.CreateCommand())
                        {
                            cmd.CommandText = "DELETE FROM suspicious_assets";
                            cmd.ExecuteNonQuery();
                        }

                        m_SuspiciousCache = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                        using (var cmd = m_Connection.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO suspicious_assets (uuid, flagged_at) VALUES (?, ?)";
                            var p1 = cmd.CreateParameter();
                            var p2 = cmd.CreateParameter();
                            cmd.Parameters.Add(p1);
                            cmd.Parameters.Add(p2);

                            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                            foreach (var uuid in uuids)
                            {
                                string nid = NormalizeUUID(uuid);
                                if (m_SuspiciousCache.Add(nid))
                                {
                                    p1.Value = nid;
                                    p2.Value = now;
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                        trans.Commit();
                    }
                    catch (Exception ex)
                    {
                        try { trans.Rollback(); } catch {}
                        m_log.Error("[ADVANCED ASSET SERVICE]: Failed to save suspicious assets: " + ex.Message);
                    }
                }
            }
        }

        private void InitializeDatabase()
        {
            lock (m_Lock)
            {
                bool exists = File.Exists(m_IndexFile);
                m_Connection = new SQLiteConnection($"Data Source={m_IndexFile};Version=3;Cache Size=20000;");
                m_Connection.Open();
                ExecuteNonQuery("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;");
                
                if (!exists)
                {
                    ExecuteNonQuery("CREATE TABLE index_assets (hash TEXT PRIMARY KEY, pack_id INTEGER, offset INTEGER, length INTEGER)");
                    ExecuteNonQuery("CREATE TABLE asset_map (uuid TEXT PRIMARY KEY COLLATE NOCASE, hash TEXT, type INTEGER, name TEXT, created INTEGER, synced INTEGER DEFAULT 0)");
                    ExecuteNonQuery("CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)");
                    ExecuteNonQuery("INSERT INTO config (key, value) VALUES ('current_pack_id', '0')");
                    ExecuteNonQuery("CREATE INDEX idx_asset_sync ON asset_map(synced)");
                }
                else
                {
                    // Migration for 'created' and 'synced' columns
                    try { ExecuteNonQuery("ALTER TABLE asset_map ADD COLUMN created INTEGER DEFAULT 0"); } catch { }
                    try { ExecuteNonQuery("ALTER TABLE asset_map ADD COLUMN synced INTEGER DEFAULT 0"); } catch { }
                    try { ExecuteNonQuery("CREATE INDEX idx_asset_sync ON asset_map(synced)"); } catch { }
                }
                ExecuteNonQuery("CREATE TABLE IF NOT EXISTS suspicious_assets (uuid TEXT PRIMARY KEY COLLATE NOCASE, flagged_at INTEGER)");
                LoadConfig();
                string activePackPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", m_CurrentPackID));
                PerformCrashRecovery(activePackPath);
            }
        }

        private void ExecuteNonQuery(string sql)
        {
            using (var cmd = m_Connection.CreateCommand()) { cmd.CommandText = sql; cmd.ExecuteNonQuery(); }
        }

        private void LoadConfig()
        {
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT value FROM config WHERE key = 'current_pack_id'";
                m_CurrentPackID = Convert.ToInt32(cmd.ExecuteScalar() ?? 0);
            }
        }

        private void PerformCrashRecovery(string activePackPath)
        {
            if (!File.Exists(activePackPath)) return;

            long lastValidOffset = 0;
            try
            {
                using (FileStream fs = new FileStream(activePackPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                using (BinaryReader br = new BinaryReader(fs))
                {
                    while (fs.Position < fs.Length)
                    {
                        long currentOffset = fs.Position;
                        
                        if (fs.Length - currentOffset < 6) break;
                        
                        uint magic = br.ReadUInt32();
                        if (magic != MAGIC_NUMBER && magic != MAGIC_NUMBER_LINK) break;
                        ushort version = br.ReadUInt16();
                        
                        long expectedHeaderRest = 19;
                        if (version >= 2) expectedHeaderRest += 8;
                        
                        if (fs.Length - fs.Position < expectedHeaderRest) break;
                        
                        br.ReadBytes(16); // UUID
                        br.ReadSByte(); // Type
                        if (version >= 2) br.ReadInt64(); // Created
                        
                        ushort nameLen = br.ReadUInt16();
                        if (fs.Length - fs.Position < nameLen) break;
                        br.ReadBytes(nameLen);
                        
                        if (magic == MAGIC_NUMBER)
                        {
                            if (fs.Length - fs.Position < 4) break;
                            int dataLen = br.ReadInt32();
                            if (dataLen < 0 || fs.Length - fs.Position < dataLen) break;
                            br.ReadBytes(dataLen);
                        }
                        else // magic == MAGIC_NUMBER_LINK
                        {
                            if (fs.Length - fs.Position < 2) break;
                            ushort hashLen = br.ReadUInt16();
                            if (fs.Length - fs.Position < hashLen) break;
                            br.ReadBytes(hashLen);
                        }
                        
                        lastValidOffset = fs.Position;
                    }

                    if (fs.Length > lastValidOffset)
                    {
                        m_log.Warn(string.Format("[AAS Recovery]: Truncating corrupted trailing bytes in active pack at offset {0}. Saved {1} bytes of corrupt data.", lastValidOffset, fs.Length - lastValidOffset));
                        fs.SetLength(lastValidOffset);
                    }
                }
            }
            catch (Exception ex)
            {
                m_log.Error(string.Format("[AAS Recovery]: Failed to scan active pack for crash recovery: {0}", ex.Message));
            }
        }

        private HashSet<string> ScanAllPhysicalUuids()
        {
            HashSet<string> physicalUuids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                string[] files = Directory.GetFiles(m_BasePath, "pack_*.bin");
                foreach (string file in files)
                {
                    using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (BinaryReader br = new BinaryReader(fs))
                    {
                        while (fs.Position < fs.Length)
                        {
                            if (fs.Length - fs.Position < 6) break;
                            
                            uint magic = br.ReadUInt32();
                            if (magic != MAGIC_NUMBER && magic != MAGIC_NUMBER_LINK) break;
                            
                            ushort version = br.ReadUInt16();
                            long expectedHeaderRest = 19;
                            if (version >= 2) expectedHeaderRest += 8;
                            
                            if (fs.Length - fs.Position < expectedHeaderRest) break;
                            
                            byte[] uuidBytes = br.ReadBytes(16);
                            string uuidStr = new UUID(uuidBytes, 0).ToString().ToLower().Replace("-", "");
                            physicalUuids.Add(uuidStr);
                            
                            br.ReadSByte(); // Type
                            if (version >= 2) br.ReadInt64(); // Created
                            
                            ushort nameLen = br.ReadUInt16();
                            if (fs.Length - fs.Position < nameLen) break;
                            br.ReadBytes(nameLen);
                            
                            if (magic == MAGIC_NUMBER)
                            {
                                if (fs.Length - fs.Position < 4) break;
                                int dataLen = br.ReadInt32();
                                if (dataLen < 0 || fs.Length - fs.Position < dataLen) break;
                                fs.Position += dataLen;
                            }
                            else // magic == MAGIC_NUMBER_LINK
                            {
                                if (fs.Length - fs.Position < 2) break;
                                ushort hashLen = br.ReadUInt16();
                                if (fs.Length - fs.Position < hashLen) break;
                                fs.Position += hashLen;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_log.Error("[AAS Migration]: Error scanning physical pack files: " + ex.Message);
            }
            return physicalUuids;
        }

        private void MigrateDuplicateLinks()
        {
            try
            {
                string migrated = "";
                lock (m_Lock)
                {
                    if (m_Disposed) return;
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT value FROM config WHERE key = 'duplicate_links_migrated'";
                        migrated = Convert.ToString(cmd.ExecuteScalar() ?? "");
                    }
                }
                if (migrated == "true") return;

                m_log.Info("[AAS Migration]: Starting database duplicate asset scan and physical pack verification...");
                
                HashSet<string> physicalUuids = ScanAllPhysicalUuids();
                if (m_Disposed) return;
                m_log.Info(string.Format("[AAS Migration]: Found {0} UUIDs physically present in pack files.", physicalUuids.Count));

                var primaryEntries = new List<PackFileIndexEntry>();
                lock (m_Lock)
                {
                    if (m_Disposed) return;
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT hash, pack_id, offset, length FROM index_assets";
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                primaryEntries.Add(new PackFileIndexEntry
                                {
                                    Hash = reader.GetString(0),
                                    PackFileID = reader.GetInt32(1),
                                    Offset = reader.GetInt64(2),
                                    Length = reader.GetInt32(3)
                                });
                            }
                        }
                    }
                }

                int migratedLinksCount = 0;
                int processedHashes = 0;

                foreach (var entry in primaryEntries)
                {
                    if (m_Disposed) return;
                    processedHashes++;
                    string primaryUuid = ReadPrimaryUuidFromPack(entry.PackFileID, entry.Offset);
                    if (primaryUuid == null) continue;

                    var duplicateUuids = new List<AssetMetadataRecord>();
                    lock (m_Lock)
                    {
                        if (m_Disposed) return;
                        using (var cmd = m_Connection.CreateCommand())
                        {
                            cmd.CommandText = "SELECT uuid, type, name, created FROM asset_map WHERE hash = ? AND uuid != ?";
                            cmd.Parameters.AddWithValue(null, entry.Hash);
                            cmd.Parameters.AddWithValue(null, primaryUuid);
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    duplicateUuids.Add(new AssetMetadataRecord
                                    {
                                        UUID = reader.GetString(0),
                                        Type = (sbyte)reader.GetInt32(1),
                                        Name = reader.IsDBNull(2) ? "" : reader.GetString(2),
                                        Created = reader.GetInt64(3)
                                    });
                                }
                            }
                        }
                    }

                    foreach (var dup in duplicateUuids)
                    {
                        if (m_Disposed) return;
                        string normalizedDupUuid = NormalizeUUID(dup.UUID);
                        if (!physicalUuids.Contains(normalizedDupUuid))
                        {
                            m_log.Info(string.Format("[AAS Migration]: Securing missing duplicate link in pack files for UUID {0} (Hash: {1})", dup.UUID, entry.Hash));
                            WriteLinkRecordPhysically(dup.UUID, entry.Hash, dup.Type, dup.Name, dup.Created);
                            physicalUuids.Add(normalizedDupUuid);
                            migratedLinksCount++;
                        }
                    }
                }

                lock (m_Lock)
                {
                    if (m_Disposed) return;
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES ('duplicate_links_migrated', 'true')";
                        cmd.ExecuteNonQuery();
                    }
                }

                m_log.Info(string.Format("[AAS Migration]: Migration completed. Secured {0} missing duplicate links across {1} unique hashes in the pack files.", migratedLinksCount, processedHashes));
            }
            catch (Exception)
            {
                if (m_Disposed)
                {
                    // Ignore exceptions if we are disposing/disposed
                    return;
                }
                throw;
            }
        }

        private string ReadPrimaryUuidFromPack(int packId, long offset)
        {
            string packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
            if (!File.Exists(packPath)) return null;

            try
            {
                using (FileStream fs = new FileStream(packPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BinaryReader br = new BinaryReader(fs))
                {
                    fs.Seek(offset, SeekOrigin.Begin);
                    if (br.ReadUInt32() != MAGIC_NUMBER) return null;
                    br.ReadUInt16(); // version
                    return new UUID(br.ReadBytes(16), 0).ToString().ToLower().Replace("-", "");
                }
            }
            catch { return null; }
        }

        private void WriteLinkRecordPhysically(string uuid, string hash, sbyte type, string name, long created)
        {
            string packPath = "";
            lock (m_Lock)
            {
                int packId = m_CurrentPackID;
                packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                if (new FileInfo(packPath).Exists && new FileInfo(packPath).Length > m_MaxPackSize)
                {
                    packId = m_CurrentPackID + 1;
                    m_CurrentPackID = packId;
                    packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                    
                    ExecuteNonQuery(string.Format("INSERT OR REPLACE INTO config (key, value) VALUES ('current_pack_id', '{0}')", m_CurrentPackID));
                }
            }

            try
            {
                using (FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                using (BinaryWriter bw = new BinaryWriter(fs))
                {
                    bw.Write(MAGIC_NUMBER_LINK);
                    bw.Write(RECORD_VERSION);
                    bw.Write(new UUID(uuid).GetBytes());
                    bw.Write(type);
                    bw.Write(created);
                    byte[] nameBytes = Encoding.UTF8.GetBytes(name ?? "");
                    bw.Write((ushort)nameBytes.Length);
                    bw.Write(nameBytes);

                    byte[] hashBytes = Encoding.UTF8.GetBytes(hash);
                    bw.Write((ushort)hashBytes.Length);
                    bw.Write(hashBytes);

                    bw.Flush();
                    fs.Flush(true);
                }
            }
            catch (Exception ex)
            {
                m_log.Error(string.Format("[AAS Migration]: Failed to write link record for {0}: {1}", uuid, ex.Message));
            }
        }

        private string NormalizeUUID(string uuid) { return uuid.ToLower().Replace("-", ""); }

        public bool AssetExists(string uuid)
        {
            string nid = NormalizeUUID(uuid);
            if (m_PendingWritesCache.ContainsKey(nid)) return true;

            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT 1 FROM asset_map WHERE uuid = :uuid LIMIT 1";
                    cmd.Parameters.AddWithValue(":uuid", nid);
                    return cmd.ExecuteScalar() != null;
                }
            }
        }

        public bool[] AssetsExist(string[] uuids)
        {
            bool[] results = new bool[uuids.Length];
            List<int> toCheck = new List<int>();
            Dictionary<string, int> nidToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < uuids.Length; i++)
            {
                if (string.IsNullOrEmpty(uuids[i])) continue;
                string nid = NormalizeUUID(uuids[i]);
                if (m_PendingWritesCache.ContainsKey(nid))
                {
                    results[i] = true;
                }
                else
                {
                    toCheck.Add(i);
                    nidToIndex[nid] = i;
                }
            }

            if (toCheck.Count > 0)
            {
                lock (m_Lock)
                {
                    for (int i = 0; i < toCheck.Count; i += 500)
                    {
                        int count = Math.Min(500, toCheck.Count - i);
                        using (var cmd = m_Connection.CreateCommand())
                        {
                            StringBuilder sb = new StringBuilder();
                            sb.Append("SELECT uuid FROM asset_map WHERE uuid IN (");
                            for (int j = 0; j < count; j++)
                            {
                                if (j > 0) sb.Append(",");
                                string paramName = ":p" + j;
                                sb.Append(paramName);
                                cmd.Parameters.AddWithValue(paramName, NormalizeUUID(uuids[toCheck[i + j]]));
                            }
                            sb.Append(")");
                            cmd.CommandText = sb.ToString();

                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    string foundNid = reader.GetString(0);
                                    if (nidToIndex.TryGetValue(foundNid, out int index))
                                    {
                                        results[index] = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return results;
        }

        private byte[] ReadAssetDataBytes(int packFileID, long offset, int length, string expectedHash, bool verifyOnRead)
        {
            string packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packFileID));
            
            // If it's the active (unsealed) pack file, use standard FileStream to avoid boundary issues with growing size.
            // Otherwise, use Memory-Mapped Files for maximum concurrency and zero-copy read performance.
            bool useMMF = packFileID < CurrentPackID;
            
            if (useMMF)
            {
                try
                {
                    var mmf = m_MappedPacks.GetOrAdd(packFileID, pid => 
                    {
                        return MemoryMappedFile.CreateFromFile(packPath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
                    });
                    
                    long mapSize = length + 1024;
                    FileInfo fi = new FileInfo(packPath);
                    if (fi.Exists)
                    {
                        long fileLen = fi.Length;
                        if (offset + mapSize > fileLen)
                        {
                            mapSize = fileLen - offset;
                        }
                    }
                    
                    using (var accessor = mmf.CreateViewAccessor(offset, mapSize, MemoryMappedFileAccess.Read))
                    {
                        if (accessor.ReadUInt32(0) != MAGIC_NUMBER) return null;
                        ushort version = accessor.ReadUInt16(4);
                        
                        long headerOffset = 4 + 2 + 16 + 1; // Magic + Version + UUID + Type
                        if (version >= 2) headerOffset += 8; // Created
                        
                        ushort nameLen = accessor.ReadUInt16(headerOffset);
                        headerOffset += 2 + nameLen; // Name bytes
                        
                        int dataLen = accessor.ReadInt32(headerOffset);
                        headerOffset += 4;
                        
                        byte[] data = new byte[dataLen];
                        accessor.ReadArray(headerOffset, data, 0, dataLen);
                        
                        if (verifyOnRead && expectedHash != null)
                        {
                            string computedHash = ComputeHash(data);
                            if (computedHash != expectedHash)
                            {
                                m_log.Error(string.Format("[ADVANCED ASSET SERVICE]: Corruption detected in asset! Hash mismatch (MMF): expected {0}, got {1}", expectedHash, computedHash));
                                return null;
                            }
                        }
                        return data;
                    }
                }
                catch (Exception ex)
                {
                    m_log.Debug($"[ADVANCED ASSET SERVICE]: MMF read failed for pack {packFileID}, falling back to FileStream. Error: {ex.Message}");
                }
            }
            
            try
            {
                using (FileStream fs = new FileStream(packPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BinaryReader br = new BinaryReader(fs))
                {
                    fs.Seek(offset, SeekOrigin.Begin);
                    if (br.ReadUInt32() != MAGIC_NUMBER) return null;
                    ushort version = br.ReadUInt16();
                    br.ReadBytes(16); // UUID
                    br.ReadSByte(); // Type
                    
                    if (version >= 2) br.ReadInt64();
                    
                    ushort nameLen = br.ReadUInt16();
                    br.ReadBytes(nameLen);
                    int dataLen = br.ReadInt32();
                    byte[] data = br.ReadBytes(dataLen);
                    
                    if (verifyOnRead && expectedHash != null)
                    {
                        string computedHash = ComputeHash(data);
                        if (computedHash != expectedHash)
                        {
                            m_log.Error(string.Format("[ADVANCED ASSET SERVICE]: Corruption detected in asset! Hash mismatch (Stream): expected {0}, got {1}", expectedHash, computedHash));
                            return null;
                        }
                    }
                    return data;
                }
            }
            catch { return null; }
        }

        public byte[] GetAssetData(string uuid, out sbyte type, out string name, bool verifyOnRead = false)
        {
            type = 0; name = string.Empty;
            string nid = NormalizeUUID(uuid);
            
            if (m_PendingWritesCache.TryGetValue(nid, out AssetWriteOp op))
            {
                type = op.Type;
                name = op.Name;
                return op.Data;
            }

            if (m_L1Cache.TryGetValue(nid, out CachedAssetInfo cached))
            {
                type = cached.Type;
                name = cached.Name;
                byte[] data = ReadAssetDataBytes(cached.PackFileID, cached.Offset, cached.Length, cached.Hash, verifyOnRead);
                if (data != null)
                {
                    ClearSuspiciousStatus(nid);
                    return data;
                }
                m_L1Cache.TryRemove(nid, out _);
            }

            CachedAssetInfo dbInfo = null;
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT am.hash, am.type, am.name, am.created, am.synced, ia.pack_id, ia.offset, ia.length " +
                                     "FROM asset_map am LEFT JOIN index_assets ia ON am.hash = ia.hash " +
                                     "WHERE am.uuid = :uuid LIMIT 1";
                    cmd.Parameters.AddWithValue(":uuid", nid);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            string hash = reader.GetString(0);
                            sbyte assetType = (sbyte)reader.GetInt32(1);
                            string assetName = reader.IsDBNull(2) ? "" : reader.GetString(2);
                            long created = reader.GetInt64(3);
                            
                            if (!reader.IsDBNull(5))
                            {
                                dbInfo = new CachedAssetInfo
                                {
                                    Hash = hash,
                                    Type = assetType,
                                    Name = assetName,
                                    Created = created,
                                    PackFileID = reader.GetInt32(5),
                                    Offset = reader.GetInt64(6),
                                    Length = reader.GetInt32(7)
                                };
                                m_L1Cache[nid] = dbInfo;
                                m_InFlightHashes[hash] = new PackFileIndexEntry
                                {
                                    Hash = hash,
                                    PackFileID = dbInfo.PackFileID,
                                    Offset = dbInfo.Offset,
                                    Length = dbInfo.Length
                                };
                            }
                            else
                            {
                                type = assetType;
                                name = assetName;
                            }
                        }
                    }
                }
            }

            if (dbInfo != null)
            {
                type = dbInfo.Type;
                name = dbInfo.Name;
                byte[] data = ReadAssetDataBytes(dbInfo.PackFileID, dbInfo.Offset, dbInfo.Length, dbInfo.Hash, verifyOnRead);
                if (data != null)
                {
                    ClearSuspiciousStatus(nid);
                    return data;
                }
            }

            return null;
        }

        public string StoreAssetData(string uuid, byte[] data, sbyte type, string name)
        {
            if (data == null) return null;
            string nid = NormalizeUUID(uuid);

            var op = new AssetWriteOp { 
                UUID = uuid, 
                Data = data, 
                Type = type, 
                Name = name, 
                Created = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Tcs = new TaskCompletionSource<string>() 
            };
            
            lock (m_StoreLock)
            {
                m_PendingWritesCache[nid] = op;
                m_WriteQueue.Add(op);
            }
            return uuid;
        }

        private void ProcessWriteQueue()
        {
            foreach (var op in m_WriteQueue.GetConsumingEnumerable())
            {
                try
                {
                    if (m_Disposed) break;
                    string hash = ComputeHash(op.Data);
                    string nid = NormalizeUUID(op.UUID);

                    bool isNewHash = false;
                    PackFileIndexEntry entry = null;

                    // 1. Content-based Deduplication (Check In-Flight and Database)
                    if (m_InFlightHashes.TryGetValue(hash, out entry) && entry != null)
                    {
                        isNewHash = false;
                    }
                    else
                    {
                        lock (m_Lock)
                        {
                            if (m_Disposed) break;
                            entry = GetIndexEntry(hash);
                            if (entry == null)
                            {
                                isNewHash = true;
                            }
                            else
                            {
                                isNewHash = false;
                                // Cache it locally for this session to avoid DB hits for duplicates
                                m_InFlightHashes[hash] = entry;
                            }
                        }
                    }

                    if (m_Disposed) break;
                    if (entry == null)
                    {
                        isNewHash = true;
                    }

                    long offset = 0;
                    int packId = 0;
                    string packPath = "";

                    if (isNewHash)
                    {
                        lock (m_Lock)
                        {
                            packId = m_CurrentPackID;
                            packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                            if (new FileInfo(packPath).Exists && new FileInfo(packPath).Length > m_MaxPackSize)
                            {
                                packId = m_CurrentPackID + 1;
                                m_CurrentPackID = packId; // Update m_CurrentPackID immediately inside the lock!
                                packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                                
                                QueueUpdate(cmd => {
                                    cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES ('current_pack_id', ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, m_CurrentPackID.ToString());
                                    cmd.ExecuteNonQuery();
                                });
                            }
                        }

                        // Physical Write (OUTSIDE lock)
                        using (FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                        using (BinaryWriter bw = new BinaryWriter(fs))
                        {
                            offset = fs.Position;
                            bw.Write(MAGIC_NUMBER); 
                            bw.Write(RECORD_VERSION);
                            bw.Write(new UUID(op.UUID).GetBytes()); 
                            bw.Write(op.Type);
                            bw.Write(op.Created); 
                            byte[] nameBytes = Encoding.UTF8.GetBytes(op.Name ?? "");
                            bw.Write((ushort)nameBytes.Length); 
                            bw.Write(nameBytes);
                            bw.Write(op.Data.Length); 
                            bw.Write(op.Data);

                            bw.Flush();
                            fs.Flush(true);
                        }

                        entry = new PackFileIndexEntry 
                        { 
                            Hash = hash, 
                            PackFileID = packId, 
                            Offset = offset, 
                            Length = op.Data.Length 
                        };
                        m_InFlightHashes[hash] = entry;
                    }
                    else
                    {
                        // Write Link Record to physical pack file (OUTSIDE lock)
                        lock (m_Lock)
                        {
                            packId = m_CurrentPackID;
                            packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                            if (new FileInfo(packPath).Exists && new FileInfo(packPath).Length > m_MaxPackSize)
                            {
                                packId = m_CurrentPackID + 1;
                                m_CurrentPackID = packId;
                                packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                                
                                QueueUpdate(cmd => {
                                    cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES ('current_pack_id', ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, m_CurrentPackID.ToString());
                                    cmd.ExecuteNonQuery();
                                });
                            }
                        }

                        using (FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                        using (BinaryWriter bw = new BinaryWriter(fs))
                        {
                            bw.Write(MAGIC_NUMBER_LINK); 
                            bw.Write(RECORD_VERSION);
                            bw.Write(new UUID(op.UUID).GetBytes()); 
                            bw.Write(op.Type);
                            bw.Write(op.Created); 
                            byte[] nameBytes = Encoding.UTF8.GetBytes(op.Name ?? "");
                            bw.Write((ushort)nameBytes.Length); 
                            bw.Write(nameBytes);
                            
                            byte[] hashBytes = Encoding.UTF8.GetBytes(hash);
                            bw.Write((ushort)hashBytes.Length);
                            bw.Write(hashBytes);

                            bw.Flush();
                            fs.Flush(true);
                        }
                    }

                    lock (m_Lock)
                    {
                        // Capture values for the closure
                        var currentEntry = entry;
                        var currentHash = hash;
                        var wasNewHash = isNewHash;

                        // 2. Queue asset_map and index_assets update atomically
                        QueueUpdate(
                            cmd => {
                                // 1. Inserção no asset_map
                                cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 0)";
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue(null, nid); 
                                cmd.Parameters.AddWithValue(null, currentHash);
                                cmd.Parameters.AddWithValue(null, (int)op.Type); 
                                cmd.Parameters.AddWithValue(null, op.Name ?? "");
                                cmd.Parameters.AddWithValue(null, op.Created);
                                cmd.ExecuteNonQuery();

                                // 2. Inserção no index_assets se for novo hash
                                if (wasNewHash)
                                {
                                    cmd.CommandText = "INSERT OR IGNORE INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, currentHash); 
                                    cmd.Parameters.AddWithValue(null, currentEntry.PackFileID);
                                    cmd.Parameters.AddWithValue(null, currentEntry.Offset); 
                                    cmd.Parameters.AddWithValue(null, op.Data.Length);
                                    cmd.ExecuteNonQuery();
                                }
                            },
                            () => {
                                m_PendingWritesCache.TryGetValue(nid, out var currentOp);
                                if (currentOp == op)
                                {
                                    m_PendingWritesCache.TryRemove(nid, out _);
                                }
                                var info = new CachedAssetInfo
                                {
                                    Hash = hash,
                                    Type = op.Type,
                                    Name = op.Name,
                                    Created = op.Created,
                                    PackFileID = entry.PackFileID,
                                    Offset = entry.Offset,
                                    Length = entry.Length
                                };
                                m_L1Cache[nid] = info;
                                op.Tcs.SetResult(currentHash);
                            },
                            () => {
                                m_PendingWritesCache.TryGetValue(nid, out var currentOp);
                                if (currentOp == op)
                                {
                                    m_PendingWritesCache.TryRemove(nid, out _);
                                }
                                if (wasNewHash)
                                {
                                    m_InFlightHashes.TryRemove(currentHash, out _);
                                }
                                op.Tcs.SetException(new Exception("Failed to commit database transaction for asset " + op.UUID));
                            }
                        );
                    }
                }
                catch (Exception ex) { 
                    if (m_Disposed) break;
                    m_log.Error("[ADVANCED ASSET SERVICE]: Background write error", ex);
                    try { m_PendingWritesCache.TryRemove(NormalizeUUID(op.UUID), out _); } catch {}
                    op.Tcs.SetException(ex);
                }
            }
        }

        private void QueueUpdate(Action<SQLiteCommand> action, Action postCommitAction = null, Action onFailureAction = null)
        {
            m_PendingUpdates.Enqueue(new PendingUpdate { Action = action, PostCommitAction = postCommitAction, OnFailureAction = onFailureAction });
            if (m_PendingUpdates.Count >= 500) FlushBatch();
        }

        private void FlushBatch()
        {
            if (m_PendingUpdates.IsEmpty) return;
            if (m_Disposed) return;
            lock (m_Lock)
            {
                if (m_Disposed) return;
                List<PendingUpdate> updates = new List<PendingUpdate>();
                while (m_PendingUpdates.TryDequeue(out var pendingUpdate))
                {
                    updates.Add(pendingUpdate);
                }

                if (updates.Count == 0) return;

                SQLiteTransaction trans = null;
                bool batchSucceeded = false;
                try
                {
                    trans = m_Connection.BeginTransaction();
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        foreach (var update in updates)
                        {
                            update.Action(cmd);
                        }
                        trans.Commit();
                        batchSucceeded = true;
                    }
                }
                catch (Exception ex)
                {
                    m_log.Warn("[ADVANCED ASSET SERVICE]: Failed to commit SQLite batch transaction, falling back to individual commits. Error: " + ex.Message);
                    try { trans?.Rollback(); } catch { }
                }
                finally
                {
                    trans?.Dispose();
                }

                if (batchSucceeded)
                {
                    foreach (var update in updates)
                    {
                        if (update.PostCommitAction != null)
                        {
                            try { update.PostCommitAction(); } catch (Exception ex) { m_log.Error("[ADVANCED ASSET SERVICE]: Error in post-commit action: " + ex.Message); }
                        }
                    }
                }
                else
                {
                    foreach (var update in updates)
                    {
                        SQLiteTransaction indTrans = null;
                        bool indSucceeded = false;
                        try
                        {
                            indTrans = m_Connection.BeginTransaction();
                            using (var cmd = m_Connection.CreateCommand())
                            {
                                update.Action(cmd);
                                indTrans.Commit();
                                indSucceeded = true;
                            }
                        }
                        catch (Exception indEx)
                        {
                            m_log.Error("[ADVANCED ASSET SERVICE]: Individual SQL update failed: " + indEx.Message);
                            try { indTrans?.Rollback(); } catch { }
                        }
                        finally
                        {
                            indTrans?.Dispose();
                        }

                        if (indSucceeded)
                        {
                            if (update.PostCommitAction != null)
                            {
                                try { update.PostCommitAction(); } catch (Exception ex) { m_log.Error("[ADVANCED ASSET SERVICE]: Error in post-commit action: " + ex.Message); }
                            }
                        }
                        else
                        {
                            if (update.OnFailureAction != null)
                            {
                                try { update.OnFailureAction(); } catch (Exception ex) { m_log.Error("[ADVANCED ASSET SERVICE]: Error in failure action: " + ex.Message); }
                            }
                        }
                    }
                }
            }
        }

        public void RebuildIndex()
        {
            lock (m_Lock)
            {
                FlushBatch();
                m_L1Cache.Clear();
                m_log.Info("[ADVANCED ASSET SERVICE]: Rebuilding index...");
                string[] files = Directory.GetFiles(m_BasePath, "pack_*.bin");
                Array.Sort(files);

                int startIndex = PromptResumeProgress("rebuild-index", "run", files.Length, out bool resume);
                if (!resume)
                {
                    ExecuteNonQuery("DELETE FROM index_assets; DELETE FROM asset_map;");
                    StartCommandProgress("rebuild-index", "run", files.Length);
                }

                for (int i = startIndex; i < files.Length; i++)
                {
                    if (CheckUserAbort())
                    {
                        m_log.Warn("[ADVANCED ASSET SERVICE]: Rebuild index aborted by user.");
                        return;
                    }
                    ScanPackFile(files[i], int.Parse(Path.GetFileNameWithoutExtension(files[i]).Substring(5)));
                    UpdateCommandProgress("rebuild-index", i + 1);
                }
                ClearCommandProgress("rebuild-index");
                m_log.Info("[ADVANCED ASSET SERVICE]: Rebuild finished.");
            }
        }

        private void ScanPackFile(string path, int packId)
        {
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (BinaryReader br = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    long offset = fs.Position;
                    try
                    {
                        uint magic = br.ReadUInt32();
                        if (magic != MAGIC_NUMBER && magic != MAGIC_NUMBER_LINK) break;
                        
                        ushort version = br.ReadUInt16();
                        string uuid = new UUID(br.ReadBytes(16), 0).ToString().ToLower().Replace("-", "");
                        sbyte type = br.ReadSByte();
                        long created = (version >= 2) ? br.ReadInt64() : 0;
                        string name = Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
                        
                        if (magic == MAGIC_NUMBER)
                        {
                            int dataLen = br.ReadInt32();
                            string hash = ComputeHash(br.ReadBytes(dataLen));
                            
                            using (var cmd = m_Connection.CreateCommand()) {
                                cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 0)";
                                cmd.Parameters.AddWithValue(null, uuid);
                                cmd.Parameters.AddWithValue(null, hash);
                                cmd.Parameters.AddWithValue(null, (int)type);
                                cmd.Parameters.AddWithValue(null, name);
                                cmd.Parameters.AddWithValue(null, created);
                                cmd.ExecuteNonQuery();
                                
                                cmd.CommandText = "INSERT OR IGNORE INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue(null, hash);
                                cmd.Parameters.AddWithValue(null, packId);
                                cmd.Parameters.AddWithValue(null, offset);
                                cmd.Parameters.AddWithValue(null, dataLen);
                                cmd.ExecuteNonQuery();
                            }
                        }
                        else // magic == MAGIC_NUMBER_LINK
                        {
                            ushort hashLen = br.ReadUInt16();
                            string hash = Encoding.UTF8.GetString(br.ReadBytes(hashLen));
                            
                            using (var cmd = m_Connection.CreateCommand()) {
                                cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 0)";
                                cmd.Parameters.AddWithValue(null, uuid);
                                cmd.Parameters.AddWithValue(null, hash);
                                cmd.Parameters.AddWithValue(null, (int)type);
                                cmd.Parameters.AddWithValue(null, name);
                                cmd.Parameters.AddWithValue(null, created);
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch { break; }
                }
            }
        }

        public List<string> Search(string pattern)
        {
            List<string> results = new List<string>();
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT uuid, name, type, created FROM asset_map WHERE name LIKE :pattern LIMIT 100";
                    cmd.Parameters.AddWithValue(":pattern", "%" + pattern + "%");
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read()) {
                            long ts = reader.GetInt64(3);
                            string dateStr = ts > 0 ? DateTimeOffset.FromUnixTimeSeconds(ts).LocalDateTime.ToString("yyyy-MM-dd HH:mm") : "Unknown";
                            results.Add($"{reader.GetString(0)} | {reader.GetString(1)} (Type: {reader.GetInt32(2)}, Added: {dateStr})");
                        }
                    }
                }
            }
            return results;
        }

        public List<AssetMetadataRecord> GetAllAssets()
        {
            List<AssetMetadataRecord> results = new List<AssetMetadataRecord>();
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT uuid, hash, type, name, created, synced FROM asset_map";
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            results.Add(new AssetMetadataRecord
                            {
                                UUID = reader.GetString(0),
                                Hash = reader.GetString(1),
                                Type = (sbyte)reader.GetInt32(2),
                                Name = reader.IsDBNull(3) ? "" : reader.GetString(3),
                                Created = reader.GetInt64(4),
                                Synced = reader.GetInt32(5) != 0
                            });
                        }
                    }
                }
            }
            return results;
        }

        public List<AssetMetadataRecord> GetUnsyncedAssets(int limit)
        {
            List<AssetMetadataRecord> results = new List<AssetMetadataRecord>();
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT uuid, hash, type, name, created FROM asset_map WHERE synced = 0 LIMIT :limit";
                    cmd.Parameters.AddWithValue(":limit", limit);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            results.Add(new AssetMetadataRecord
                            {
                                UUID = reader.GetString(0),
                                Hash = reader.GetString(1),
                                Type = (sbyte)reader.GetInt32(2),
                                Name = reader.IsDBNull(3) ? "" : reader.GetString(3),
                                Created = reader.GetInt64(4)
                            });
                        }
                    }
                }
            }
            return results;
        }

        public void MarkAsSynced(string uuid)
        {
            string nid = NormalizeUUID(uuid);
            QueueUpdate(cmd => {
                cmd.CommandText = "UPDATE asset_map SET synced = 1 WHERE uuid = ?";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, nid);
                cmd.ExecuteNonQuery();
            });
        }

        private string ComputeHash(byte[] data)
        {
            using (SHA256 sha = SHA256.Create()) return BitConverter.ToString(sha.ComputeHash(data)).Replace("-", "").ToLower();
        }

        private AssetMetadataRecord GetMetadata(string uuid)
        {
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT hash, type, name, created, synced FROM asset_map WHERE uuid = :uuid";
                cmd.Parameters.AddWithValue(":uuid", uuid);
                using (var reader = cmd.ExecuteReader())
                    if (reader.Read()) return new AssetMetadataRecord { 
                        Hash = reader.GetString(0), 
                        Type = (sbyte)reader.GetInt32(1), 
                        Name = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        Created = reader.GetInt64(3),
                        Synced = reader.GetInt32(4) != 0
                    };
            }
            return null;
        }

        private PackFileIndexEntry GetIndexEntry(string hash)
        {
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT pack_id, offset, length FROM index_assets WHERE hash = :hash";
                cmd.Parameters.AddWithValue(":hash", hash);
                using (var reader = cmd.ExecuteReader())
                    if (reader.Read()) return new PackFileIndexEntry { Hash = hash, PackFileID = reader.GetInt32(0), Offset = reader.GetInt64(1), Length = reader.GetInt32(2) };
            }
            return null;
        }

        public void Dispose()
        {
            lock (m_Lock)
            {
                if (m_Disposed) return;
                m_Disposed = true;
            }
            m_WriteQueue.CompleteAdding();
            m_BatchTimer?.Stop();
            if (m_WriteTask != null)
            {
                try { m_WriteTask.Wait(10000); } catch { }
            }
            FlushBatch();
            if (m_Connection != null) { m_Connection.Close(); m_Connection.Dispose(); m_Connection = null; }
            foreach (var mmf in m_MappedPacks.Values)
            {
                try { mmf.Dispose(); } catch { }
            }
            m_MappedPacks.Clear();
        }

        public void BackupDatabase(string destinationPath)
        {
            lock (m_Lock)
            {
                FlushBatch();
                using (var destinationConnection = new SQLiteConnection($"Data Source={destinationPath};Version=3;"))
                {
                    destinationConnection.Open();
                    m_Connection.BackupDatabase(destinationConnection, "main", "main", -1, null, 0);
                }
            }
        }

        public Dictionary<string, object> GetStats()
        {
            var stats = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            lock (m_Lock)
            {
                // 1. Database sizes and files
                stats["BasePath"] = m_BasePath;
                stats["IndexFile"] = m_IndexFile;
                
                try
                {
                    if (File.Exists(m_IndexFile))
                        stats["IndexFileSize"] = new FileInfo(m_IndexFile).Length;
                    else
                        stats["IndexFileSize"] = 0L;
                }
                catch { stats["IndexFileSize"] = -1L; }

                // 2. Packfiles size and count
                try
                {
                    string[] files = Directory.GetFiles(m_BasePath, "pack_*.bin");
                    stats["PackFilesCount"] = (long)files.Length;
                    long totalSize = 0;
                    foreach (var file in files)
                    {
                        totalSize += new FileInfo(file).Length;
                    }
                    stats["PackFilesTotalSize"] = totalSize;
                }
                catch
                {
                    stats["PackFilesCount"] = -1L;
                    stats["PackFilesTotalSize"] = -1L;
                }

                // 3. Database counts
                try
                {
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT COUNT(*) FROM index_assets";
                        stats["TotalUniqueDataBlocks"] = (long)cmd.ExecuteScalar();
                    }
                }
                catch { stats["TotalUniqueDataBlocks"] = -1L; }

                try
                {
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT COUNT(*) FROM asset_map";
                        stats["TotalUniqueAssets"] = (long)cmd.ExecuteScalar();
                    }
                }
                catch { stats["TotalUniqueAssets"] = -1L; }

                try
                {
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT COUNT(*) FROM suspicious_assets";
                        stats["TotalSuspiciousAssets"] = (long)cmd.ExecuteScalar();
                    }
                }
                catch { stats["TotalSuspiciousAssets"] = -1L; }

                try
                {
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT COUNT(*) FROM asset_map WHERE synced = 0";
                        stats["TotalUnsyncedAssets"] = (long)cmd.ExecuteScalar();
                    }
                }
                catch { stats["TotalUnsyncedAssets"] = -1L; }
            }

            return stats;
        }

        public void VerifyIntegrity(Action<string> output)
        {
            output("Starting AdvancedAssetService Integrity Verification...");
            FlushBatch();

            int totalAssets = 0;
            int missingPacks = 0;
            int corruptedAssets = 0;
            int validAssets = 0;
            int missingLinks = 0;

            lock (m_Lock)
            {
                // 1. Verify Links (asset_map -> index_assets)
                output("Phase 1: Verifying database links...");
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT COUNT(*) FROM asset_map WHERE hash NOT IN (SELECT hash FROM index_assets)";
                    missingLinks = Convert.ToInt32(cmd.ExecuteScalar() ?? 0);
                    if (missingLinks > 0)
                        output($"[WARNING] Found {missingLinks} UUIDs pointing to missing data hashes.");
                    else
                        output("All UUID links are valid.");
                }

                // 2. Verify Hashes and physical data
                output("Phase 2: Verifying physical data integrity...");
                List<PackFileIndexEntry> entries = new List<PackFileIndexEntry>();
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT hash, pack_id, offset, length FROM index_assets";
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            entries.Add(new PackFileIndexEntry
                            {
                                Hash = reader.GetString(0),
                                PackFileID = reader.GetInt32(1),
                                Offset = reader.GetInt64(2),
                                Length = reader.GetInt32(3)
                            });
                        }
                    }
                }

                totalAssets = entries.Count;
                output($"Total unique data blocks (Hashes) to verify: {totalAssets}");

                int startIndex = PromptResumeProgress("verify", "run", totalAssets, out bool resume);
                if (!resume)
                {
                    StartCommandProgress("verify", "run", totalAssets);
                }
                else
                {
                    string metadata = GetConfig("cmd_state:verify:metadata");
                    if (!string.IsNullOrEmpty(metadata))
                    {
                        string[] parts = metadata.Split(',');
                        if (parts.Length == 3)
                        {
                            int.TryParse(parts[0], out missingPacks);
                            int.TryParse(parts[1], out corruptedAssets);
                            int.TryParse(parts[2], out validAssets);
                        }
                    }
                }

                Dictionary<int, FileStream> openPacks = new Dictionary<int, FileStream>();

                try
                {
                    for (int i = startIndex; i < entries.Count; i++)
                    {
                        if (CheckUserAbort())
                        {
                            output("Integrity verification aborted by user.");
                            return;
                        }
                        var entry = entries[i];
                        if ((i + 1) % 1000 == 0 || i + 1 == entries.Count)
                        {
                            output($"Verified {i + 1} / {totalAssets}...");
                            UpdateCommandProgress("verify", i + 1);
                            SetConfig("cmd_state:verify:metadata", string.Format("{0},{1},{2}", missingPacks, corruptedAssets, validAssets));
                        }

                        if (!openPacks.TryGetValue(entry.PackFileID, out FileStream fs))
                        {
                            string packPath = Path.Combine(m_BasePath, $"pack_{entry.PackFileID}.bin");
                            if (!File.Exists(packPath))
                            {
                                missingPacks++;
                                output($"[ERROR] Pack file missing: {packPath}");
                                continue;
                            }
                            fs = new FileStream(packPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                            openPacks[entry.PackFileID] = fs;
                        }

                        try
                        {
                            using (BinaryReader br = new BinaryReader(fs, Encoding.UTF8, true))
                            {
                                fs.Seek(entry.Offset, SeekOrigin.Begin);
                                if (br.ReadUInt32() != MAGIC_NUMBER)
                                {
                                    corruptedAssets++;
                                    output($"[ERROR] Invalid magic number at offset {entry.Offset} in pack {entry.PackFileID} (Hash: {entry.Hash})");
                                    continue;
                                }

                                ushort version = br.ReadUInt16();
                                br.ReadBytes(16); // UUID
                                br.ReadSByte(); // Type
                                if (version >= 2) br.ReadInt64(); // Created
                                
                                // Name string was written as: ushort (length) + bytes
                                ushort nameLen = br.ReadUInt16();
                                br.ReadBytes(nameLen); // Skip Name
                                
                                int dataLen = br.ReadInt32();
                                if (dataLen != entry.Length)
                                {
                                    corruptedAssets++;
                                    output($"[ERROR] Length mismatch for Hash: {entry.Hash}");
                                    continue;
                                }

                                byte[] data = br.ReadBytes(dataLen);
                                string computedHash = ComputeHash(data);
                                if (computedHash != entry.Hash)
                                {
                                    corruptedAssets++;
                                    output($"[ERROR] Hash mismatch! Expected {entry.Hash}, got {computedHash}");
                                }
                                else
                                {
                                    validAssets++;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            corruptedAssets++;
                            output($"[ERROR] Read error for Hash {entry.Hash}: {ex.Message}");
                        }
                    }
                    ClearCommandProgress("verify");
                }
                finally
                {
                    foreach (var fs in openPacks.Values) fs.Dispose();
                }
            }

            output("--- Verification Summary ---");
            output($"Total Data Blocks: {totalAssets}");
            output($"Valid Blocks:      {validAssets}");
            output($"Missing Packs:     {missingPacks}");
            output($"Corrupted Blocks:  {corruptedAssets}");
            output($"Broken UUID Links: {missingLinks}");
            if (corruptedAssets == 0 && missingPacks == 0 && missingLinks == 0)
                output("STATUS: PERFECT. No issues found.");
            else
                output("STATUS: ISSUES DETECTED.");
        }

        private class DefragRecord
        {
            public string UUID;
            public string Hash;
            public int PackFileID;
            public long Offset;
            public int Length;
        }

        public void Defragment(IFSAssetDataPlugin gridConnector, Action<string> output)
        {
            output("Starting AdvancedAssetService PackFile Defragmentation...");
            lock (m_Lock)
            {
                FlushBatch();
                
                // 1. Get all active mappings (UUID -> Hash) and their physical entries, excluding suspicious assets
                List<DefragRecord> activeEntries = new List<DefragRecord>();
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT am.uuid, ia.hash, ia.pack_id, ia.offset, ia.length FROM index_assets ia INNER JOIN asset_map am ON am.hash = ia.hash WHERE am.uuid NOT IN (SELECT uuid FROM suspicious_assets)";
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            activeEntries.Add(new DefragRecord
                            {
                                UUID = reader.GetString(0),
                                Hash = reader.GetString(1),
                                PackFileID = reader.GetInt32(2),
                                Offset = reader.GetInt64(3),
                                Length = reader.GetInt32(4)
                            });
                        }
                    }
                }

                if (activeEntries.Count == 0)
                {
                    output("No active assets found. Defragmentation skipped.");
                    return;
                }

                output(string.Format("Found {0} active assets to defragment.", activeEntries.Count));

                // Defrag progress/resume setup
                ExecuteNonQuery("CREATE TABLE IF NOT EXISTS defrag_new_entries (hash TEXT, pack_id INTEGER, offset INTEGER, length INTEGER)");
                ExecuteNonQuery("CREATE TABLE IF NOT EXISTS defrag_new_maps (uuid TEXT, hash TEXT, type INTEGER, name TEXT, created INTEGER)");

                int startIndex = PromptResumeProgress("defrag", "run", activeEntries.Count, out bool resume);

                string tempDir = Path.Combine(m_BasePath, "defrag_tmp");
                List<PackFileIndexEntry> newEntries = new List<PackFileIndexEntry>();
                List<AssetMetadataRecord> newMapEntries = new List<AssetMetadataRecord>();
                var writtenHashMetadata = new Dictionary<string, AssetMetadataRecord>(StringComparer.OrdinalIgnoreCase);
                int currentDestPackId = 0;

                if (!resume)
                {
                    ExecuteNonQuery("DELETE FROM defrag_new_entries; DELETE FROM defrag_new_maps;");
                    try { if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true); } catch {}
                    Directory.CreateDirectory(tempDir);
                    StartCommandProgress("defrag", "run", activeEntries.Count);
                }
                else
                {
                    // Load existing state
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT hash, pack_id, offset, length FROM defrag_new_entries";
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                newEntries.Add(new PackFileIndexEntry
                                {
                                    Hash = reader.GetString(0),
                                    PackFileID = reader.GetInt32(1),
                                    Offset = reader.GetInt64(2),
                                    Length = reader.GetInt32(3)
                                });
                            }
                        }

                        cmd.CommandText = "SELECT uuid, hash, type, name, created FROM defrag_new_maps";
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                newMapEntries.Add(new AssetMetadataRecord
                                {
                                    UUID = reader.GetString(0),
                                    Hash = reader.GetString(1),
                                    Type = (sbyte)reader.GetInt32(2),
                                    Name = reader.IsDBNull(3) ? "" : reader.GetString(3),
                                    Created = reader.GetInt64(4)
                                });
                            }
                        }
                    }

                    // Populate writtenHashMetadata from loaded newMapEntries
                    foreach (var nm in newMapEntries)
                    {
                        writtenHashMetadata[nm.Hash] = nm;
                    }

                    // Find max pack ID in loaded entries
                    foreach (var ne in newEntries)
                    {
                        if (ne.PackFileID > currentDestPackId)
                            currentDestPackId = ne.PackFileID;
                    }
                    currentDestPackId++; // Start next pack file to avoid corrupting/appending to existing bin file
                    if (!Directory.Exists(tempDir)) Directory.CreateDirectory(tempDir);
                }

                string currentDestPackPath = Path.Combine(tempDir, string.Format("pack_{0}.bin", currentDestPackId));
                long currentDestOffset = 0;
                Dictionary<int, FileStream> openSourcePacks = new Dictionary<int, FileStream>();

                FileStream destFs = new FileStream(currentDestPackPath, FileMode.Create, FileAccess.Write, FileShare.None);
                BinaryWriter destBw = new BinaryWriter(destFs);

                try
                {
                    SQLiteTransaction trans = m_Connection.BeginTransaction();
                    try
                    {
                        for (int i = startIndex; i < activeEntries.Count; i++)
                        {
                            if (CheckUserAbort())
                            {
                                output("Defragmentation aborted by user.");
                                return;
                            }
                            var entry = activeEntries[i];

                            try
                            {
                                // Check if UUID is invalid (e.g. SHA-256 hash of 64 chars) and try to repair it via MySQL
                                string correctedUuid = entry.UUID;
                                if (entry.UUID.Length > 36)
                                {
                                    string resolvedUuid = null;
                                    if (gridConnector != null)
                                    {
                                        try
                                        {
                                            resolvedUuid = gridConnector.GetUUIDByHash(entry.Hash);
                                        }
                                        catch {}
                                    }

                                    if (!string.IsNullOrEmpty(resolvedUuid) && UUID.TryParse(resolvedUuid, out UUID dummyId))
                                    {
                                        correctedUuid = dummyId.ToString().ToLower().Replace("-", "");
                                        output(string.Format("Defrag Resolved invalid UUID '{0}' to correct UUID '{1}' via grid database.", entry.UUID, dummyId));
                                    }
                                    else
                                    {
                                        output(string.Format("Defrag Skipping invalid UUID '{0}' (no mapping found in grid database).", entry.UUID));
                                        
                                        // Still update progress and commit periodically when skipping
                                        if ((i + 1) % 500 == 0 || (i + 1) == activeEntries.Count)
                                        {
                                            trans.Commit();
                                            trans.Dispose();
                                            UpdateCommandProgress("defrag", i + 1);
                                            output(string.Format("Processed {0} / {1}...", i + 1, activeEntries.Count));
                                            if ((i + 1) < activeEntries.Count)
                                            {
                                                trans = m_Connection.BeginTransaction();
                                            }
                                        }
                                        continue;
                                    }
                                }

                                // Skip duplicate asset if it has already been written
                                if (writtenHashMetadata.TryGetValue(entry.Hash, out var existingMeta))
                                {
                                    var nm = new AssetMetadataRecord
                                    {
                                        UUID = correctedUuid,
                                        Hash = entry.Hash,
                                        Type = existingMeta.Type,
                                        Name = existingMeta.Name,
                                        Created = existingMeta.Created
                                    };

                                    newMapEntries.Add(nm);

                                    // Persist only in defrag_new_maps temp table since defrag_new_entries already contains this hash
                                    using (var cmd = m_Connection.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO defrag_new_maps (uuid, hash, type, name, created) VALUES (?, ?, ?, ?, ?)";
                                        cmd.Parameters.AddWithValue(null, nm.UUID);
                                        cmd.Parameters.AddWithValue(null, nm.Hash);
                                        cmd.Parameters.AddWithValue(null, (int)nm.Type);
                                        cmd.Parameters.AddWithValue(null, nm.Name);
                                        cmd.Parameters.AddWithValue(null, nm.Created);
                                        cmd.ExecuteNonQuery();
                                    }

                                    if ((i + 1) % 500 == 0 || (i + 1) == activeEntries.Count)
                                    {
                                        trans.Commit();
                                        trans.Dispose();
                                        UpdateCommandProgress("defrag", i + 1);
                                        output(string.Format("Processed {0} / {1}...", i + 1, activeEntries.Count));
                                        if ((i + 1) < activeEntries.Count)
                                        {
                                            trans = m_Connection.BeginTransaction();
                                        }
                                    }
                                    continue;
                                }

                                // Read from source pack
                                if (!openSourcePacks.TryGetValue(entry.PackFileID, out FileStream sourceFs))
                                {
                                    string sourcePackPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", entry.PackFileID));
                                    if (!File.Exists(sourcePackPath))
                                    {
                                        output(string.Format("[ERROR] Source pack file missing: {0}. Skipping this asset.", sourcePackPath));
                                        
                                        // Still update progress and commit periodically when skipping
                                        if ((i + 1) % 500 == 0 || (i + 1) == activeEntries.Count)
                                        {
                                            trans.Commit();
                                            trans.Dispose();
                                            UpdateCommandProgress("defrag", i + 1);
                                            output(string.Format("Processed {0} / {1}...", i + 1, activeEntries.Count));
                                            if ((i + 1) < activeEntries.Count)
                                            {
                                                trans = m_Connection.BeginTransaction();
                                            }
                                        }
                                        continue;
                                    }
                                    sourceFs = new FileStream(sourcePackPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                                    openSourcePacks[entry.PackFileID] = sourceFs;
                                }

                                sourceFs.Seek(entry.Offset, SeekOrigin.Begin);
                                using (BinaryReader sourceBr = new BinaryReader(sourceFs, Encoding.UTF8, true))
                                {
                                    if (sourceBr.ReadUInt32() != MAGIC_NUMBER)
                                    {
                                        output(string.Format("[ERROR] Magic number mismatch for hash {0}. Skipping.", entry.Hash));
                                        
                                        // Still update progress and commit periodically when skipping
                                        if ((i + 1) % 500 == 0 || (i + 1) == activeEntries.Count)
                                        {
                                            trans.Commit();
                                            trans.Dispose();
                                            UpdateCommandProgress("defrag", i + 1);
                                            output(string.Format("Processed {0} / {1}...", i + 1, activeEntries.Count));
                                            if ((i + 1) < activeEntries.Count)
                                            {
                                                trans = m_Connection.BeginTransaction();
                                            }
                                        }
                                        continue;
                                    }
                                    ushort version = sourceBr.ReadUInt16();
                                    byte[] uuidBytes = sourceBr.ReadBytes(16);
                                    sbyte type = sourceBr.ReadSByte();
                                    long created = (version >= 2) ? sourceBr.ReadInt64() : 0;
                                    ushort nameLen = sourceBr.ReadUInt16();
                                    byte[] nameBytes = sourceBr.ReadBytes(nameLen);
                                    int dataLen = sourceBr.ReadInt32();
                                    byte[] dataBytes = sourceBr.ReadBytes(dataLen);

                                    string name = Encoding.UTF8.GetString(nameBytes);

                                    byte[] finalUuidBytes = uuidBytes;
                                    if (correctedUuid != entry.UUID)
                                    {
                                        finalUuidBytes = new UUID(correctedUuid).GetBytes();
                                    }

                                    // Write to dest pack
                                    if (destFs.Position > m_MaxPackSize)
                                    {
                                        destBw.Dispose();
                                        destFs.Dispose();
                                        currentDestPackId++;
                                        currentDestPackPath = Path.Combine(tempDir, string.Format("pack_{0}.bin", currentDestPackId));
                                        destFs = new FileStream(currentDestPackPath, FileMode.Create, FileAccess.Write, FileShare.None);
                                        destBw = new BinaryWriter(destFs);
                                    }

                                    currentDestOffset = destFs.Position;
                                    destBw.Write(MAGIC_NUMBER);
                                    destBw.Write(version);
                                    destBw.Write(finalUuidBytes);
                                    destBw.Write(type);
                                    if (version >= 2) destBw.Write(created);
                                    destBw.Write(nameLen);
                                    destBw.Write(nameBytes);
                                    destBw.Write(dataLen);
                                    destBw.Write(dataBytes);

                                    // Query and write link records for duplicate UUIDs mapping to this hash
                                    using (var cmdLinks = m_Connection.CreateCommand())
                                    {
                                        cmdLinks.CommandText = "SELECT uuid, type, name, created FROM asset_map WHERE hash = ? AND uuid != ?";
                                        cmdLinks.Parameters.AddWithValue(null, entry.Hash);
                                        cmdLinks.Parameters.AddWithValue(null, entry.UUID); // exclude primary
                                        using (var readerLinks = cmdLinks.ExecuteReader())
                                        {
                                            while (readerLinks.Read())
                                            {
                                                string linkUuid = readerLinks.GetString(0);
                                                sbyte linkType = (sbyte)readerLinks.GetInt32(1);
                                                string linkName = readerLinks.IsDBNull(2) ? "" : readerLinks.GetString(2);
                                                long linkCreated = readerLinks.GetInt64(3);

                                                byte[] linkNameBytes = Encoding.UTF8.GetBytes(linkName);
                                                byte[] linkHashBytes = Encoding.UTF8.GetBytes(entry.Hash);

                                                destBw.Write(MAGIC_NUMBER_LINK);
                                                destBw.Write(version);
                                                destBw.Write(new UUID(linkUuid).GetBytes());
                                                destBw.Write(linkType);
                                                destBw.Write(linkCreated);
                                                destBw.Write((ushort)linkNameBytes.Length);
                                                destBw.Write(linkNameBytes);
                                                destBw.Write((ushort)linkHashBytes.Length);
                                                destBw.Write(linkHashBytes);
                                            }
                                        }
                                    }

                                    var ne = new PackFileIndexEntry
                                    {
                                        Hash = entry.Hash,
                                        PackFileID = currentDestPackId,
                                        Offset = currentDestOffset,
                                        Length = dataLen
                                    };
                                    var nm = new AssetMetadataRecord
                                    {
                                        UUID = correctedUuid,
                                        Hash = entry.Hash,
                                        Type = type,
                                        Name = name,
                                        Created = created
                                    };

                                    newEntries.Add(ne);
                                    newMapEntries.Add(nm);
                                    writtenHashMetadata[entry.Hash] = nm;

                                    // Persist in temp tables
                                    using (var cmd = m_Connection.CreateCommand())
                                    {
                                        cmd.CommandText = "INSERT INTO defrag_new_entries (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                        cmd.Parameters.AddWithValue(null, ne.Hash);
                                        cmd.Parameters.AddWithValue(null, ne.PackFileID);
                                        cmd.Parameters.AddWithValue(null, ne.Offset);
                                        cmd.Parameters.AddWithValue(null, ne.Length);
                                        cmd.ExecuteNonQuery();

                                        cmd.CommandText = "INSERT INTO defrag_new_maps (uuid, hash, type, name, created) VALUES (?, ?, ?, ?, ?)";
                                        cmd.Parameters.Clear();
                                        cmd.Parameters.AddWithValue(null, nm.UUID);
                                        cmd.Parameters.AddWithValue(null, nm.Hash);
                                        cmd.Parameters.AddWithValue(null, (int)nm.Type);
                                        cmd.Parameters.AddWithValue(null, nm.Name);
                                        cmd.Parameters.AddWithValue(null, nm.Created);
                                        cmd.ExecuteNonQuery();
                                    }

                                    if ((i + 1) % 500 == 0 || (i + 1) == activeEntries.Count)
                                    {
                                        trans.Commit();
                                        trans.Dispose();
                                        UpdateCommandProgress("defrag", i + 1);
                                        output(string.Format("Processed {0} / {1}...", i + 1, activeEntries.Count));
                                        if ((i + 1) < activeEntries.Count)
                                        {
                                            trans = m_Connection.BeginTransaction();
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                output(string.Format("[ERROR] Failed to read/write record for hash {0}: {1}", entry.Hash, ex.Message));
                            }
                        }
                    }
                    catch
                    {
                        try { trans.Rollback(); } catch {}
                        trans.Dispose();
                        throw;
                    }

                    destBw.Dispose();
                    destFs.Dispose();

                    // Close all source files
                    foreach (var fs in openSourcePacks.Values) fs.Dispose();
                    openSourcePacks.Clear();

                    // Move new pack files
                    output("Updating index database and applying file changes...");
                    string[] oldPackFiles = Directory.GetFiles(m_BasePath, "pack_*.bin");
                    foreach (var file in oldPackFiles)
                    {
                        try { File.Delete(file); } catch (Exception ex) { output(string.Format("[WARNING] Failed to delete old pack {0}: {1}", file, ex.Message)); }
                    }

                    string[] newPackFiles = Directory.GetFiles(tempDir, "pack_*.bin");
                    foreach (var file in newPackFiles)
                    {
                        string destPath = Path.Combine(m_BasePath, Path.GetFileName(file));
                        File.Move(file, destPath);
                    }

                    Directory.Delete(tempDir, true);

                    // Update SQLite database in transaction
                    using (var finalTrans = m_Connection.BeginTransaction())
                    {
                        try
                        {
                            using (var cmd = m_Connection.CreateCommand())
                            {
                                cmd.CommandText = "DELETE FROM index_assets; DELETE FROM asset_map;";
                                cmd.ExecuteNonQuery();

                                cmd.CommandText = "INSERT INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                foreach (var ne in newEntries)
                                {
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, ne.Hash);
                                    cmd.Parameters.AddWithValue(null, ne.PackFileID);
                                    cmd.Parameters.AddWithValue(null, ne.Offset);
                                    cmd.Parameters.AddWithValue(null, ne.Length);
                                    cmd.ExecuteNonQuery();
                                }

                                cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 1)";
                                foreach (var nm in newMapEntries)
                                {
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, nm.UUID);
                                    cmd.Parameters.AddWithValue(null, nm.Hash);
                                    cmd.Parameters.AddWithValue(null, (int)nm.Type);
                                    cmd.Parameters.AddWithValue(null, nm.Name);
                                    cmd.Parameters.AddWithValue(null, nm.Created);
                                    cmd.ExecuteNonQuery();
                                }

                                m_CurrentPackID = currentDestPackId;
                                cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES ('current_pack_id', ?)";
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue(null, m_CurrentPackID.ToString());
                                cmd.ExecuteNonQuery();
                                cmd.CommandText = "DROP TABLE IF EXISTS defrag_new_entries; DROP TABLE IF EXISTS defrag_new_maps; DELETE FROM suspicious_assets;";
                                cmd.ExecuteNonQuery();
                            }
                            ClearCommandProgress("defrag");
                            m_SuspiciousCache = null;
                            finalTrans.Commit();
                        }
                        catch (Exception ex)
                        {
                            finalTrans.Rollback();
                            output("[FATAL ERROR] Failed to commit SQLite transaction for defragmentation: " + ex.Message);
                            throw;
                        }
                    }

                    output(string.Format("Defragmentation finished. Compacted into {0} packfile(s).", m_CurrentPackID + 1));
                }
                finally
                {
                    foreach (var fs in openSourcePacks.Values) fs.Dispose();
                    try { if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true); } catch {}
                    m_L1Cache.Clear();
                    foreach (var mmf in m_MappedPacks.Values)
                    {
                        try { mmf.Dispose(); } catch { }
                    }
                    m_MappedPacks.Clear();
                }
            }
        }

        private void ScanPackFileResilient(string path, int packId, Action<string> output)
        {
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                long length = fs.Length;
                while (fs.Position < length)
                {
                    long offset = fs.Position;
                    try
                    {
                        using (BinaryReader br = new BinaryReader(fs, Encoding.UTF8, true))
                        {
                            uint magic = br.ReadUInt32();
                            if (magic != MAGIC_NUMBER && magic != MAGIC_NUMBER_LINK)
                            {
                                fs.Seek(offset + 1, SeekOrigin.Begin);
                                continue;
                            }

                            ushort version = br.ReadUInt16();
                            byte[] uuidBytes = br.ReadBytes(16);
                            string uuid = new UUID(uuidBytes, 0).ToString().ToLower().Replace("-", "");
                            sbyte type = br.ReadSByte();
                            long created = (version >= 2) ? br.ReadInt64() : 0;
                            ushort nameLen = br.ReadUInt16();
                            string name = Encoding.UTF8.GetString(br.ReadBytes(nameLen));

                            if (magic == MAGIC_NUMBER)
                            {
                                int dataLen = br.ReadInt32();
                                byte[] dataBytes = br.ReadBytes(dataLen);
                                string hash = ComputeHash(dataBytes);

                                using (var cmd = m_Connection.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 0)";
                                    cmd.Parameters.AddWithValue(null, uuid);
                                    cmd.Parameters.AddWithValue(null, hash);
                                    cmd.Parameters.AddWithValue(null, (int)type);
                                    cmd.Parameters.AddWithValue(null, name);
                                    cmd.Parameters.AddWithValue(null, created);
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = "INSERT OR IGNORE INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, hash);
                                    cmd.Parameters.AddWithValue(null, packId);
                                    cmd.Parameters.AddWithValue(null, offset);
                                    cmd.Parameters.AddWithValue(null, dataLen);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            else // magic == MAGIC_NUMBER_LINK
                            {
                                ushort hashLen = br.ReadUInt16();
                                string hash = Encoding.UTF8.GetString(br.ReadBytes(hashLen));

                                using (var cmd = m_Connection.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 0)";
                                    cmd.Parameters.AddWithValue(null, uuid);
                                    cmd.Parameters.AddWithValue(null, hash);
                                    cmd.Parameters.AddWithValue(null, (int)type);
                                    cmd.Parameters.AddWithValue(null, name);
                                    cmd.Parameters.AddWithValue(null, created);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        output(string.Format("[DEEP SCAN WARNING] Read error at offset {0} in pack {1}: {2}. Attempting to salvage next record...", offset, packId, ex.Message));
                        try
                        {
                            fs.Seek(offset + 1, SeekOrigin.Begin);
                        }
                        catch
                        {
                            break;
                        }
                    }
                }
            }
        }

        public void RebuildIndexResilient(Action<string> output)
        {
            lock (m_Lock)
            {
                FlushBatch();
                output("Starting AdvancedAssetService Deep Resilient Index Rebuild...");
                string[] files = Directory.GetFiles(m_BasePath, "pack_*.bin");
                Array.Sort(files);

                int startIndex = PromptResumeProgress("deep-repair", "run", files.Length, out bool resume);
                if (!resume)
                {
                    ExecuteNonQuery("DELETE FROM index_assets; DELETE FROM asset_map;");
                    StartCommandProgress("deep-repair", "run", files.Length);
                }

                for (int i = startIndex; i < files.Length; i++)
                {
                    if (CheckUserAbort())
                    {
                        output("Deep Resilient Index Rebuild aborted by user.");
                        return;
                    }
                    string file = files[i];
                    int packId = int.Parse(Path.GetFileNameWithoutExtension(file).Substring(5));
                    output(string.Format("Salvaging packfile: {0}...", Path.GetFileName(file)));
                    ScanPackFileResilient(file, packId, output);
                    UpdateCommandProgress("deep-repair", i + 1);
                }
                ClearCommandProgress("deep-repair");
                output("Deep Resilient Index Rebuild finished.");
            }
        }

        public List<KeyValuePair<string, string>> GetBrokenLinks()
        {
            List<KeyValuePair<string, string>> results = new List<KeyValuePair<string, string>>();
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT uuid, hash FROM asset_map WHERE hash NOT IN (SELECT hash FROM index_assets)";
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            results.Add(new KeyValuePair<string, string>(reader.GetString(0), reader.GetString(1)));
                        }
                    }
                }
            }
            return results;
        }

        public void SetConfig(string key, string value)
        {
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)";
                    cmd.Parameters.AddWithValue(null, key);
                    cmd.Parameters.AddWithValue(null, value);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public string GetConfig(string key)
        {
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT value FROM config WHERE key = ?";
                    cmd.Parameters.AddWithValue(null, key);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return reader.GetString(0);
                        }
                    }
                }
            }
            return null;
        }

        public void DeleteConfig(string key)
        {
            lock (m_Lock)
            {
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "DELETE FROM config WHERE key = ?";
                    cmd.Parameters.AddWithValue(null, key);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public int PromptResumeProgress(string commandName, string key, int totalItems, out bool resume)
        {
            resume = false;
            string status = GetConfig("cmd_state:" + commandName + ":status");
            if (status == "running")
            {
                string savedKey = GetConfig("cmd_state:" + commandName + ":key");
                if (savedKey == key)
                {
                    string processedStr = GetConfig("cmd_state:" + commandName + ":processed");
                    if (int.TryParse(processedStr, out int processed) && processed > 0 && processed < totalItems)
                    {
                        string promptMsg = "An interrupted '" + commandName + "' operation was found at " + processed + "/" + totalItems + ". Do you want to resume from where you left off?";
                        if (OpenSim.Framework.MainConsole.Instance.Prompt(promptMsg, "yes") == "yes")
                        {
                            resume = true;
                            return processed;
                        }
                    }
                }
            }
            ClearCommandProgress(commandName);
            return 0;
        }

        public void StartCommandProgress(string commandName, string key, int totalItems)
        {
            SetConfig("cmd_state:" + commandName + ":status", "running");
            SetConfig("cmd_state:" + commandName + ":key", key);
            SetConfig("cmd_state:" + commandName + ":total", totalItems.ToString());
            SetConfig("cmd_state:" + commandName + ":processed", "0");
        }

        public void UpdateCommandProgress(string commandName, int processed)
        {
            SetConfig("cmd_state:" + commandName + ":processed", processed.ToString());
        }

        public void ClearCommandProgress(string commandName)
        {
            DeleteConfig("cmd_state:" + commandName + ":status");
            DeleteConfig("cmd_state:" + commandName + ":key");
            DeleteConfig("cmd_state:" + commandName + ":total");
            DeleteConfig("cmd_state:" + commandName + ":processed");
            DeleteConfig("cmd_state:" + commandName + ":metadata");
        }

        public bool CheckUserAbort()
        {
            try
            {
                if (Console.KeyAvailable)
                {
                    ConsoleKeyInfo key = Console.ReadKey(true);
                    if (key.Key == ConsoleKey.Escape)
                    {
                        OpenSim.Framework.MainConsole.Instance.Output("Operation aborted by user (Escape key pressed).");
                        return true;
                    }
                }
            }
            catch (InvalidOperationException)
            {
                // Console input is redirected/headless, ignore key checks
            }
            return false;
        }
    }
}
