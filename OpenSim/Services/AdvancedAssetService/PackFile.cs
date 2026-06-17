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
    public class AssetMetadataRecord { public string UUID; public string Hash; public sbyte Type; public string Name; public long Created; }
    
    public class AssetWriteOp
    {
        public string UUID;
        public byte[] Data;
        public sbyte Type;
        public string Name;
        public long Created;
        public TaskCompletionSource<string> Tcs;
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
        private ConcurrentQueue<Action<SQLiteCommand>> m_PendingUpdates = new ConcurrentQueue<Action<SQLiteCommand>>();
        private BlockingCollection<AssetWriteOp> m_WriteQueue = new BlockingCollection<AssetWriteOp>(5000);

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
                    ExecuteNonQuery("CREATE TABLE asset_map (uuid TEXT PRIMARY KEY COLLATE NOCASE, hash TEXT, type INTEGER, name TEXT, created INTEGER)");
                    ExecuteNonQuery("CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)");
                    ExecuteNonQuery("INSERT INTO config (key, value) VALUES ('current_pack_id', '0')");
                }
                else
                {
                    // Migration for 'created' column
                    try { ExecuteNonQuery("ALTER TABLE asset_map ADD COLUMN created INTEGER DEFAULT 0"); } catch { }
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

        public byte[] GetAssetData(string uuid, out sbyte type, out string name)
        {
            type = 0; name = string.Empty;
            string nid = NormalizeUUID(uuid);
            lock (m_Lock)
            {
                AssetMetadataRecord meta = GetMetadata(nid);
                if (meta == null) return null;
                type = meta.Type; name = meta.Name;
                PackFileIndexEntry entry = GetIndexEntry(meta.Hash);
                if (entry == null) return null;

                string packPath = Path.Combine(m_BasePath, $"pack_{entry.PackFileID}.bin");
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
                        return br.ReadBytes(dataLen);
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
                        QueueUpdate(cmd => {
                            cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created) VALUES (?, ?, ?, ?, ?)";
                            cmd.Parameters.Clear();
                            cmd.Parameters.AddWithValue(null, nid); 
                            cmd.Parameters.AddWithValue(null, hash);
                            cmd.Parameters.AddWithValue(null, (int)op.Type); 
                            cmd.Parameters.AddWithValue(null, op.Name ?? "");
                            cmd.Parameters.AddWithValue(null, op.Created);
                            cmd.ExecuteNonQuery();
                        });

                        if (GetIndexEntry(hash) == null)
                        {
                            string packPath = Path.Combine(m_BasePath, $"pack_{m_CurrentPackID}.bin");
                            if (new FileInfo(packPath).Exists && new FileInfo(packPath).Length > m_MaxPackSize)
                            {
                                m_CurrentPackID++;
                                QueueUpdate(cmd => {
                                    cmd.CommandText = "INSERT OR REPLACE INTO config (key, value) VALUES ('current_pack_id', ?)";
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue(null, m_CurrentPackID.ToString());
                                    cmd.ExecuteNonQuery();
                                });
                                packPath = Path.Combine(m_BasePath, $"pack_{m_CurrentPackID}.bin");
                            }

                            long offset = 0;
                            using (FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                            using (BinaryWriter bw = new BinaryWriter(fs))
                            {
                                offset = fs.Position;
                                bw.Write(MAGIC_NUMBER); 
                                bw.Write(RECORD_VERSION);
                                bw.Write(new UUID(op.UUID).GetBytes()); 
                                bw.Write(op.Type);
                                bw.Write(op.Created); // V2 Feature
                                byte[] nameBytes = Encoding.UTF8.GetBytes(op.Name ?? "");
                                bw.Write((ushort)nameBytes.Length); 
                                bw.Write(nameBytes);
                                bw.Write(op.Data.Length); 
                                bw.Write(op.Data);
                            }

                            QueueUpdate(cmd => {
                                cmd.CommandText = "INSERT OR IGNORE INTO index_assets (hash, pack_id, offset, length) VALUES (?, ?, ?, ?)";
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue(null, hash); 
                                cmd.Parameters.AddWithValue(null, m_CurrentPackID);
                                cmd.Parameters.AddWithValue(null, offset); 
                                cmd.Parameters.AddWithValue(null, op.Data.Length);
                                cmd.ExecuteNonQuery();
                            });
                        }
                    }
                    op.Tcs.SetResult(hash);
                }
                catch (Exception ex) { 
                    m_log.Error("[ADVANCED ASSET SERVICE]: Background write error: " + ex.Message);
                    op.Tcs.SetException(ex);
                }
            }
        }

        private void QueueUpdate(Action<SQLiteCommand> action)
        {
            m_PendingUpdates.Enqueue(action);
            if (m_PendingUpdates.Count >= 500) FlushBatch();
        }

        private void FlushBatch()
        {
            if (m_PendingUpdates.IsEmpty) return;
            lock (m_Lock)
            {
                using (var trans = m_Connection.BeginTransaction())
                using (var cmd = m_Connection.CreateCommand())
                {
                    while (m_PendingUpdates.TryDequeue(out var action)) { try { action(cmd); } catch { } }
                    trans.Commit();
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
                            cmd.CommandText = "INSERT OR REPLACE INTO asset_map (uuid, hash, type, name, created) VALUES (?, ?, ?, ?, ?)";
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
                    cmd.CommandText = "SELECT uuid, hash, type, name, created FROM asset_map";
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

        private string ComputeHash(byte[] data)
        {
            using (SHA256 sha = SHA256.Create()) return BitConverter.ToString(sha.ComputeHash(data)).Replace("-", "").ToLower();
        }

        private AssetMetadataRecord GetMetadata(string uuid)
        {
            using (var cmd = m_Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT hash, type, name, created FROM asset_map WHERE uuid = :uuid";
                cmd.Parameters.AddWithValue(":uuid", uuid);
                using (var reader = cmd.ExecuteReader())
                    if (reader.Read()) return new AssetMetadataRecord { 
                        Hash = reader.GetString(0), 
                        Type = (sbyte)reader.GetInt32(1), 
                        Name = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        Created = reader.GetInt64(3)
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

        public void VerifyIntegrity(Action<string> p) { /* Implementation logic here */ }
    }
}
