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

namespace OpenSim.Region.CoreModules.Asset
{
    public class PackCacheEntry { public string Hash; public long Offset; public int Length; public int PackFileID; }

    public class PendingUpdate
    {
        public Action<SQLiteCommand> Action;
        public Action PostCommitAction;
    }

    public class PendingCacheEntry
    {
        public byte[] Data;
        public sbyte Type;
        public string Name;
        public PackCacheEntry Entry;
    }
    
    public class PackFileCache : IDisposable
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private const uint MAGIC_NUMBER = 0x41414321; // "AAC!" (Advanced Asset Cache)
        private const ushort VERSION = 1;

        private string m_BasePath;
        private string m_IndexFile;
        private SQLiteConnection m_Connection;
        private int m_CurrentPackID = 0;
        private long m_MaxPackSize;
        private long m_MaxCacheSize;
        private object m_Lock = new object();
        
        private Timer m_BatchTimer;
        private ConcurrentQueue<PendingUpdate> m_PendingUpdates = new ConcurrentQueue<PendingUpdate>();
        private Dictionary<string, PendingCacheEntry> m_PendingReads = new Dictionary<string, PendingCacheEntry>();

        public PackFileCache(string basePath, long maxSize, long maxPackSize)
        {
            m_BasePath = basePath;
            m_MaxCacheSize = maxSize;
            m_MaxPackSize = maxPackSize;
            if (!Directory.Exists(m_BasePath)) Directory.CreateDirectory(m_BasePath);
            m_IndexFile = Path.Combine(m_BasePath, "cache_index.db");
            InitializeDatabase();

            m_BatchTimer = new Timer(5000);
            m_BatchTimer.AutoReset = true;
            m_BatchTimer.Elapsed += (s, e) => FlushBatch();
            m_BatchTimer.Start();
        }

        private void InitializeDatabase()
        {
            lock (m_Lock)
            {
                bool exists = File.Exists(m_IndexFile);
                m_Connection = new SQLiteConnection($"Data Source={m_IndexFile};Version=3;Cache Size=10000;");
                m_Connection.Open();
                ExecuteNonQuery("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY;");
                
                if (!exists)
                {
                    ExecuteNonQuery("CREATE TABLE index_assets (hash TEXT PRIMARY KEY, pack_id INTEGER, offset INTEGER, length INTEGER)");
                    ExecuteNonQuery("CREATE TABLE asset_map (uuid TEXT PRIMARY KEY COLLATE NOCASE, hash TEXT, type INTEGER, name TEXT, last_access INTEGER)");
                    ExecuteNonQuery("CREATE TABLE packs (id INTEGER PRIMARY KEY, size INTEGER, created INTEGER)");
                    ExecuteNonQuery("CREATE INDEX idx_asset_access ON asset_map(last_access)");
                }
                LoadLatestPack();
            }
        }

        private void ExecuteNonQuery(string sql)
        {
            using (var cmd = m_Connection.CreateCommand()) { cmd.CommandText = sql; cmd.ExecuteNonQuery(); }
        }

        private void LoadLatestPack()
        {
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT id FROM packs ORDER BY id DESC LIMIT 1";
                m_CurrentPackID = Convert.ToInt32(cmd.ExecuteScalar() ?? 0);
            }
        }

        public byte[] Get(string uuid, out sbyte type, out string name)
        {
            type = 0; name = string.Empty;
            string nid = uuid.ToLower().Replace("-", "");
            
            lock (m_Lock)
            {
                if (m_PendingReads.TryGetValue(nid, out PendingCacheEntry pending))
                {
                    type = pending.Type;
                    name = pending.Name;
                    return pending.Data;
                }

                PackCacheEntry entry = null;
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT m.hash, m.type, m.name, i.pack_id, i.offset, i.length " +
                                     "FROM asset_map m JOIN index_assets i ON m.hash = i.hash " +
                                     "WHERE m.uuid = :uuid LIMIT 1";
                    cmd.Parameters.AddWithValue(":uuid", nid);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            entry = new PackCacheEntry {
                                Hash = reader.GetString(0),
                                PackFileID = reader.GetInt32(3),
                                Offset = reader.GetInt64(4),
                                Length = reader.GetInt32(5)
                            };
                            type = (sbyte)reader.GetInt32(1);
                            name = reader.IsDBNull(2) ? "" : reader.GetString(2);
                        }
                    }
                }

                if (entry == null) return null;

                // Update last access time (queued)
                long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                QueueUpdate(cmd => {
                    cmd.CommandText = "UPDATE asset_map SET last_access = ? WHERE uuid = ?";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue(null, now);
                    cmd.Parameters.AddWithValue(null, nid);
                    cmd.ExecuteNonQuery();
                });

                byte[] data = ReadFromPack(entry);
                if (data == null)
                {
                    m_log.WarnFormat("[ADVANCED ASSET CACHE]: Asset {0} read from pack {1} offset {2} failed or is corrupted. Removing metadata to allow re-caching.", uuid, entry.PackFileID, entry.Offset);
                    RemoveAssetMetadata(nid, entry.Hash);
                    return null;
                }
                return data;
            }
        }

        private void RemoveAssetMetadata(string uuid, string hash)
        {
            QueueUpdate(cmd => {
                cmd.CommandText = "DELETE FROM asset_map WHERE uuid = ?";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, uuid);
                cmd.ExecuteNonQuery();

                // Only delete from index_assets if no other asset maps to it
                cmd.CommandText = "SELECT 1 FROM asset_map WHERE hash = ? LIMIT 1";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, hash);
                if (cmd.ExecuteScalar() == null)
                {
                    cmd.CommandText = "DELETE FROM index_assets WHERE hash = ?";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue(null, hash);
                    cmd.ExecuteNonQuery();
                }
            });
        }

        private byte[] ReadFromPack(PackCacheEntry entry)
        {
            string packPath = Path.Combine(m_BasePath, string.Format("cache_pack_{0}.bin", entry.PackFileID));
            if (!File.Exists(packPath)) return null;

            try
            {
                using (FileStream fs = new FileStream(packPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BinaryReader br = new BinaryReader(fs))
                {
                    fs.Seek(entry.Offset, SeekOrigin.Begin);
                    if (br.ReadUInt32() != MAGIC_NUMBER) return null;
                    if (br.ReadUInt16() != VERSION) return null;
                    br.ReadBytes(16); // UUID
                    br.ReadSByte(); // Type
                    fs.Seek(br.ReadUInt16(), SeekOrigin.Current); // Skip Name
                    int dataLen = br.ReadInt32();
                    if (dataLen != entry.Length) return null; // Corrupted length
                    byte[] data = br.ReadBytes(dataLen);
                    if (data == null || data.Length != dataLen) return null;
                    return data;
                }
            }
            catch { return null; }
        }

        public void Store(string uuid, byte[] data, sbyte type, string name)
        {
            if (data == null) return;
            string hash = ComputeHash(data);
            string nid = uuid.ToLower().Replace("-", "");
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            lock (m_Lock)
            {
                // Check if already in cache (by hash)
                if (HashExists(hash))
                {
                    var pending = new PendingCacheEntry
                    {
                        Data = data,
                        Type = type,
                        Name = name,
                        Entry = null
                    };
                    m_PendingReads[nid] = pending;

                    UpdateAssetMap(nid, hash, type, name, now);
                    return;
                }

                // Prepare new pack if needed
                CheckRotation();

                string packPath = Path.Combine(m_BasePath, string.Format("cache_pack_{0}.bin", m_CurrentPackID));
                long offset = 0;
                
                using (FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                using (BinaryWriter bw = new BinaryWriter(fs))
                {
                    offset = fs.Position;
                    bw.Write(MAGIC_NUMBER);
                    bw.Write(VERSION);
                    bw.Write(new UUID(uuid).GetBytes());
                    bw.Write(type);
                    byte[] nameBytes = Encoding.UTF8.GetBytes(name ?? "");
                    bw.Write((ushort)nameBytes.Length);
                    bw.Write(nameBytes);
                    bw.Write(data.Length);
                    bw.Write(data);
                }

                var pendingNew = new PendingCacheEntry
                {
                    Data = data,
                    Type = type,
                    Name = name,
                    Entry = new PackCacheEntry
                    {
                        Hash = hash,
                        PackFileID = m_CurrentPackID,
                        Offset = offset,
                        Length = data.Length
                    }
                };
                m_PendingReads[nid] = pendingNew;

                UpdateIndex(hash, m_CurrentPackID, offset, data.Length);
                UpdateAssetMap(nid, hash, type, name, now);
                UpdatePackSize(m_CurrentPackID, data.Length + 32 + (name?.Length ?? 0));
            }
        }

        private bool HashExists(string hash)
        {
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT 1 FROM index_assets WHERE hash = :hash LIMIT 1";
                cmd.Parameters.AddWithValue(":hash", hash);
                return cmd.ExecuteScalar() != null;
            }
        }

        private void UpdateAssetMap(string uuid, string hash, sbyte type, string name, long access)
        {
            QueueUpdate(cmd => {
                cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, last_access) VALUES (?, ?, ?, ?, ?)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, uuid);
                cmd.Parameters.AddWithValue(null, hash);
                cmd.Parameters.AddWithValue(null, (int)type);
                cmd.Parameters.AddWithValue(null, name ?? "");
                cmd.Parameters.AddWithValue(null, access);
                cmd.ExecuteNonQuery();
            }, () => {
                lock (m_Lock)
                {
                    m_PendingReads.Remove(uuid);
                }
            });
        }

        private void UpdateIndex(string hash, int packId, long offset, int length)
        {
            QueueUpdate(cmd => {
                cmd.CommandText = "INSERT OR IGNORE INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, hash);
                cmd.Parameters.AddWithValue(null, packId);
                cmd.Parameters.AddWithValue(null, offset);
                cmd.Parameters.AddWithValue(null, length);
                cmd.ExecuteNonQuery();
            });
        }

        private void UpdatePackSize(int packId, long added)
        {
            QueueUpdate(cmd => {
                cmd.CommandText = "INSERT OR IGNORE INTO packs (id, size, created) VALUES (?, 0, ?)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, packId);
                cmd.Parameters.AddWithValue(null, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                cmd.ExecuteNonQuery();

                cmd.CommandText = "UPDATE packs SET size = size + ? WHERE id = ?";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue(null, added);
                cmd.Parameters.AddWithValue(null, packId);
                cmd.ExecuteNonQuery();
            });
        }

        private void CheckRotation()
        {
            string packPath = Path.Combine(m_BasePath, string.Format("cache_pack_{0}.bin", m_CurrentPackID));
            if (File.Exists(packPath) && new FileInfo(packPath).Length > m_MaxPackSize)
            {
                m_CurrentPackID++;
                m_log.Debug(string.Format("[ADVANCED ASSET CACHE]: Rotating to new pack {0}", m_CurrentPackID));
            }

            // Simple Cleanup if total size exceeded
            CheckCacheLimit();
        }

        private void CheckCacheLimit()
        {
            long totalSize = 0;
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT SUM(size) FROM packs";
                object result = cmd.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    totalSize = Convert.ToInt64(result);
                }
            }

            if (totalSize > m_MaxCacheSize)
            {
                // Delete oldest pack
                int oldestId = -1;
                using (var cmd = m_Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT id FROM packs ORDER BY id ASC LIMIT 1";
                    var res = cmd.ExecuteScalar();
                    if (res != null) oldestId = Convert.ToInt32(res);
                }

                if (oldestId != -1 && oldestId < m_CurrentPackID)
                {
                    m_log.Info(string.Format("[ADVANCED ASSET CACHE]: Cache limit exceeded. Deleting oldest pack {0}", oldestId));
                    DeletePack(oldestId);
                }
            }
        }

        private void DeletePack(int id)
        {
            string packPath = Path.Combine(m_BasePath, string.Format("cache_pack_{0}.bin", id));
            if (File.Exists(packPath)) File.Delete(packPath);

            ExecuteNonQuery(string.Format("DELETE FROM index_assets WHERE pack_id = {0}", id));
            ExecuteNonQuery(string.Format("DELETE FROM packs WHERE id = {0}", id));
            // Clean up asset_map entries that no longer have a valid index entry
            ExecuteNonQuery("DELETE FROM asset_map WHERE hash NOT IN (SELECT hash FROM index_assets)");
        }

        private string ComputeHash(byte[] data)
        {
            using (SHA256 sha = SHA256.Create()) return BitConverter.ToString(sha.ComputeHash(data)).Replace("-", "").ToLower();
        }

        private void QueueUpdate(Action<SQLiteCommand> action, Action postCommitAction = null)
        {
            m_PendingUpdates.Enqueue(new PendingUpdate { Action = action, PostCommitAction = postCommitAction });
            if (m_PendingUpdates.Count >= 200) FlushBatch();
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
                                m_log.Error("[ADVANCED ASSET CACHE]: Error executing batch update: " + ex.Message);
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
                    m_log.Error("[ADVANCED ASSET CACHE]: Failed to commit SQLite batch transaction: " + ex.Message);
                    try { trans?.Rollback(); } catch { }
                }
                finally
                {
                    trans?.Dispose();
                }
            }
        }

        public void Clear()
        {
            lock (m_Lock)
            {
                FlushBatch();
                m_Connection.Close();
                m_Connection.Dispose();
                
                foreach (string file in Directory.GetFiles(m_BasePath, "cache_pack_*.bin")) File.Delete(file);
                if (File.Exists(m_IndexFile)) File.Delete(m_IndexFile);
                
                InitializeDatabase();
            }
        }

        public void Dispose()
        {
            m_BatchTimer?.Stop();
            FlushBatch();
            if (m_Connection != null) { m_Connection.Close(); m_Connection.Dispose(); m_Connection = null; }
        }
    }
}
