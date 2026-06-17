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

namespace OpenSim.Services.AdvancedAssetService
{
    public class PackFileIndexEntry { public string Hash; public long Offset; public int Length; public int PackFileID; }
    public class AssetMetadataRecord { public string UUID; public string Hash; public sbyte Type; public string Name; public long Created; public bool Synced; }
    
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
    }

    public class PackFileManager : IDisposable
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private const uint MAGIC_NUMBER = 0x21534141; // "AAS!"
        private const ushort RECORD_VERSION = 2; // Version 2 includes Creation Date

        private string m_BasePath;
        private string m_IndexFile;
        private SQLiteConnection m_Connection;
        private int m_CurrentPackID = 0;
        private long m_MaxPackSize = 512 * 1024 * 1024;
        private object m_Lock = new object();
        
        private Timer m_BatchTimer;
        private ConcurrentQueue<PendingUpdate> m_PendingUpdates = new ConcurrentQueue<PendingUpdate>();
        private BlockingCollection<AssetWriteOp> m_WriteQueue = new BlockingCollection<AssetWriteOp>(5000);
        private ConcurrentDictionary<string, AssetWriteOp> m_PendingWritesCache = new ConcurrentDictionary<string, AssetWriteOp>(StringComparer.OrdinalIgnoreCase);

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

            Task.Factory.StartNew(ProcessWriteQueue, TaskCreationOptions.LongRunning);
        }

        private void InitializeDatabase()
        {
            lock (m_Lock)
            {
                bool exists = File.Exists(m_IndexFile);
                m_Connection = new SQLiteConnection($"Data Source={m_IndexFile};Version=3;Cache Size=20000;");
                m_Connection.Open();
                ExecuteNonQuery("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY;");
                
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
                LoadConfig();
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

        private string NormalizeUUID(string uuid) { return uuid.ToLower().Replace("-", ""); }

        public bool AssetExists(string uuid)
        {
            string nid = NormalizeUUID(uuid);
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

            lock (m_Lock)
            {
                AssetMetadataRecord meta = GetMetadata(nid);
                if (meta == null) return null;
                type = meta.Type; name = meta.Name;
                PackFileIndexEntry entry = GetIndexEntry(meta.Hash);
                if (entry == null) return null;

                string packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", entry.PackFileID));
                try
                {
                    using (FileStream fs = new FileStream(packPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (BinaryReader br = new BinaryReader(fs))
                    {
                        fs.Seek(entry.Offset, SeekOrigin.Begin);
                        if (br.ReadUInt32() != MAGIC_NUMBER) return null;
                        ushort version = br.ReadUInt16();
                        br.ReadBytes(16); // UUID
                        br.ReadSByte(); // Type
                        
                        if (version >= 2) br.ReadInt64(); // Skip CreationDate in reader for now
                        
                        fs.Seek(br.ReadUInt16(), SeekOrigin.Current); // Skip Name
                        int dataLen = br.ReadInt32();
                        byte[] data = br.ReadBytes(dataLen);
                        
                        if (verifyOnRead)
                        {
                            string computedHash = ComputeHash(data);
                            if (computedHash != meta.Hash)
                            {
                                m_log.Error(string.Format("[ADVANCED ASSET SERVICE]: Corruption detected in asset {0}! Hash mismatch: expected {1}, got {2}", uuid, meta.Hash, computedHash));
                                return null;
                            }
                        }
                        return data;
                    }
                }
                catch { return null; }
            }
        }

        public string StoreAssetData(string uuid, byte[] data, sbyte type, string name)
        {
            if (data == null) return null;
            var op = new AssetWriteOp { 
                UUID = uuid, 
                Data = data, 
                Type = type, 
                Name = name, 
                Created = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Tcs = new TaskCompletionSource<string>() 
            };
            
            string nid = NormalizeUUID(uuid);
            m_PendingWritesCache[nid] = op;

            m_WriteQueue.Add(op);
            return uuid;
        }

        private void ProcessWriteQueue()
        {
            foreach (var op in m_WriteQueue.GetConsumingEnumerable())
            {
                try
                {
                    string hash = ComputeHash(op.Data);
                    string nid = NormalizeUUID(op.UUID);

                    lock (m_Lock)
                    {
                        bool isNewHash = GetIndexEntry(hash) == null;
                        long offset = 0;
                        int packId = m_CurrentPackID;
                        bool packChanged = false;

                        if (isNewHash)
                        {
                            string packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", m_CurrentPackID));
                            if (new FileInfo(packPath).Exists && new FileInfo(packPath).Length > m_MaxPackSize)
                            {
                                packId = m_CurrentPackID + 1;
                                packChanged = true;
                                packPath = Path.Combine(m_BasePath, string.Format("pack_{0}.bin", packId));
                            }

                            // 1. Physical Write (can throw exception if disk full or IO error)
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
                            }

                            if (packChanged)
                            {
                                m_CurrentPackID = packId;
                                QueueUpdate(cmd => {
                                    cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES ('current_pack_id', ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, m_CurrentPackID.ToString());
                                    cmd.ExecuteNonQuery();
                                });
                            }
                        }

                        // 2. Queue asset_map and index_assets update atomically
                        QueueUpdate(
                            cmd => {
                                // 1. Inserção no asset_map
                                cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created, synced) VALUES (?, ?, ?, ?, ?, 0)";
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue(null, nid); 
                                cmd.Parameters.AddWithValue(null, hash);
                                cmd.Parameters.AddWithValue(null, (int)op.Type); 
                                cmd.Parameters.AddWithValue(null, op.Name ?? "");
                                cmd.Parameters.AddWithValue(null, op.Created);
                                cmd.ExecuteNonQuery();

                                // 2. Inserção no index_assets se for novo hash
                                if (isNewHash)
                                {
                                    cmd.CommandText = "INSERT OR IGNORE INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, hash); 
                                    cmd.Parameters.AddWithValue(null, packId);
                                    cmd.Parameters.AddWithValue(null, offset); 
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
                            }
                        );
                    }
                    op.Tcs.SetResult(hash);
                }
                catch (Exception ex) { 
                    m_log.Error("[ADVANCED ASSET SERVICE]: Background write error: " + ex.Message);
                    try { m_PendingWritesCache.TryRemove(NormalizeUUID(op.UUID), out _); } catch {}
                    op.Tcs.SetException(ex);
                }
            }
        }

        private void QueueUpdate(Action<SQLiteCommand> action, Action postCommitAction = null)
        {
            m_PendingUpdates.Enqueue(new PendingUpdate { Action = action, PostCommitAction = postCommitAction });
            if (m_PendingUpdates.Count >= 500) FlushBatch();
        }

        private void FlushBatch()
        {
            if (m_PendingUpdates.IsEmpty) return;
            lock (m_Lock)
            {
                SQLiteTransaction trans = null;
                try
                {
                    trans = m_Connection.BeginTransaction();
                    using (var cmd = m_Connection.CreateCommand())
                    {
                        List<Action> postCommitActions = new List<Action>();
                        while (m_PendingUpdates.TryDequeue(out var pendingUpdate)) 
                        { 
                            try 
                            { 
                                pendingUpdate.Action(cmd); 
                                if (pendingUpdate.PostCommitAction != null)
                                    postCommitActions.Add(pendingUpdate.PostCommitAction);
                            } 
                            catch (Exception ex)
                            {
                                m_log.Error("[ADVANCED ASSET SERVICE]: Error executing batch update: " + ex.Message);
                            }
                        }
                        trans.Commit();

                        foreach (var action in postCommitActions)
                        {
                            try { action(); } catch { }
                        }
                    }
                }
                catch (Exception ex)
                {
                    m_log.Error("[ADVANCED ASSET SERVICE]: Failed to commit SQLite batch transaction: " + ex.Message);
                    try { trans?.Rollback(); } catch { }
                }
                finally
                {
                    trans?.Dispose();
                }
            }
        }

        public void RebuildIndex()
        {
            lock (m_Lock)
            {
                FlushBatch();
                m_log.Info("[ADVANCED ASSET SERVICE]: Rebuilding index...");
                ExecuteNonQuery("DELETE FROM index_assets; DELETE FROM asset_map;");
                string[] files = Directory.GetFiles(m_BasePath, "pack_*.bin");
                Array.Sort(files);
                foreach (string file in files) ScanPackFile(file, int.Parse(Path.GetFileNameWithoutExtension(file).Substring(5)));
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
                        if (br.ReadUInt32() != MAGIC_NUMBER) break;
                        ushort version = br.ReadUInt16();
                        string uuid = new UUID(br.ReadBytes(16), 0).ToString().ToLower().Replace("-", "");
                        sbyte type = br.ReadSByte();
                        long created = (version >= 2) ? br.ReadInt64() : 0;
                        string name = Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
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
            m_WriteQueue.CompleteAdding();
            m_BatchTimer?.Stop();
            FlushBatch();
            if (m_Connection != null) { m_Connection.Close(); m_Connection.Dispose(); m_Connection = null; }
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

                Dictionary<int, FileStream> openPacks = new Dictionary<int, FileStream>();

                try
                {
                    int processed = 0;
                    foreach (var entry in entries)
                    {
                        processed++;
                        if (processed % 1000 == 0) output($"Verified {processed} / {totalAssets}...");

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
    }
}
