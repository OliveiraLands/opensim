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
        public Action OnFailureAction;
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

            AssetMetadataRecord meta;
            PackFileIndexEntry entry;

            lock (m_Lock)
            {
                meta = GetMetadata(nid);
                if (meta == null) return null;
                type = meta.Type; name = meta.Name;
                entry = GetIndexEntry(meta.Hash);
            }

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
                    
                    ushort nameLen = br.ReadUInt16();
                    br.ReadBytes(nameLen); // Skip Name
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
                    string hash = ComputeHash(op.Data);
                    string nid = NormalizeUUID(op.UUID);

                    bool isNewHash = false;
                    PackFileIndexEntry entry = null;

                    // 1. Content-based Deduplication (Check In-Flight and Database)
                    if (m_InFlightHashes.TryGetValue(hash, out entry))
                    {
                        isNewHash = false;
                    }
                    else
                    {
                        lock (m_Lock)
                        {
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
                    m_log.Error("[ADVANCED ASSET SERVICE]: Background write error: " + ex.Message);
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
            lock (m_Lock)
            {
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
            if (m_WriteTask != null)
            {
                try { m_WriteTask.Wait(10000); } catch { }
            }
            FlushBatch();
            if (m_Connection != null) { m_Connection.Close(); m_Connection.Dispose(); m_Connection = null; }
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
