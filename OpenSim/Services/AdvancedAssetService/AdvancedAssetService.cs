/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using log4net;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Services.Base;
using OpenSim.Services.Interfaces;
using System.IO.Compression;
using OpenSim.Data;
using System.Timers;

namespace OpenSim.Services.AdvancedAssetService
{
    public class AdvancedAssetService : ServiceBase, IAssetService, IDisposable
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string m_StoragePath;
        protected IAssetService m_FallbackService;
        protected bool m_VerifyOnRead = true;
        
        private PackFileManager m_PackManager;
        private IFSAssetDataPlugin m_GridConnector;
        private Timer m_SyncTimer;
        private bool m_IsSyncing = false;
        private S3BackgroundReplicator m_S3Replicator;

        public AdvancedAssetService(IConfigSource config) : this(config, "AssetService")
        {
        }

        public AdvancedAssetService(IConfigSource config, string configName) : base(config)
        {
            IConfig assetConfig = config.Configs[configName];
            if (assetConfig == null)
                throw new Exception("No AssetService configuration");

            m_StoragePath = assetConfig.GetString("StoragePath", "asset_packs");
            m_VerifyOnRead = assetConfig.GetBoolean("VerifyOnRead", true);

            // Initialize Pack Manager
            m_PackManager = new PackFileManager(m_StoragePath);

            // Initialize Grid Connector (Shadow Sync)
            string dllName = assetConfig.GetString("StorageProvider", string.Empty);
            string connectionString = assetConfig.GetString("ConnectionString", string.Empty);
            string realm = assetConfig.GetString("Realm", "fsassets");

            if (!string.IsNullOrEmpty(dllName) && !string.IsNullOrEmpty(connectionString))
            {
                m_log.Info("[ADVANCED ASSET SERVICE]: Shadow Sync enabled to " + connectionString);
                m_GridConnector = LoadPlugin<IFSAssetDataPlugin>(dllName);
                if (m_GridConnector != null)
                {
                    m_GridConnector.Initialise(connectionString, realm, 0);
                    m_SyncTimer = new Timer(30000); // Sync every 30 seconds
                    m_SyncTimer.AutoReset = true;
                    m_SyncTimer.Elapsed += (s, e) => ProcessShadowSync();
                    m_SyncTimer.Start();
                }
            }

            // Setup Fallback Service
            string fallback = assetConfig.GetString("FallbackService", string.Empty);
            if (!string.IsNullOrEmpty(fallback))
            {
                m_FallbackService = LoadPlugin<IAssetService>(fallback, new object[] { config });
            }

            RegisterCommands();
            m_S3Replicator = new S3BackgroundReplicator(assetConfig, m_StoragePath);
            m_log.Info("[ADVANCED ASSET SERVICE]: Initialized with storage at " + m_StoragePath);
        }

        private void RegisterCommands()
        {
            if (MainConsole.Instance == null) return;
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas export-asset", "aas export-asset <ID> <path>", "Export an asset to a file", HandleExportAsset);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas import-asset", "aas import-asset <path> <type> <name>", "Import an asset from a file", HandleImportAsset);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas import-legacy", "aas import-legacy <path>", "Bulk import assets from FSAssetService structure", HandleImportLegacy);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas export-legacy", "aas export-legacy <path>", "Bulk export assets to FSAssetService structure", HandleExportLegacy);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas search-content", "aas search-content <string>", "Search assets for content", HandleSearchContent);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas verify", "aas verify", "Verify all assets integrity", HandleVerify);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas rebuild-index", "aas rebuild-index", "Rebuild the SQLite index from PackFiles", HandleRebuildIndex);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas import-cache", "aas import-cache <path>", "Bulk import assets from Flotsam file cache", HandleImportCache);
        }

        public virtual AssetBase Get(string id)
        {
            sbyte type;
            string name;
            byte[] data = m_PackManager.GetAssetData(id, out type, out name, m_VerifyOnRead);
            if (data != null)
            {
                AssetBase asset = new AssetBase(new UUID(id), name, type, UUID.Zero.ToString());
                asset.Data = data;
                return asset;
            }

            if (m_FallbackService != null)
            {
                AssetBase fallbackAsset = m_FallbackService.Get(id);
                if (fallbackAsset != null)
                {
                    // Optionally migrate to local pack if found in fallback?
                    // For now just return it.
                    return fallbackAsset;
                }
            }

            m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Asset {0} not found", id);
            return null;
        }

        public virtual AssetBase Get(string id, string ForeignAssetService, bool StoreOnLocalGrid)
        {
            return Get(id);
        }

        public virtual AssetBase GetCached(string id)
        {
            return Get(id);
        }

        public virtual AssetMetadata GetMetadata(string id)
        {
            AssetBase asset = Get(id);
            return asset?.Metadata;
        }

        public virtual byte[] GetData(string id)
        {
            sbyte type;
            string name;
            return m_PackManager.GetAssetData(id, out type, out name, m_VerifyOnRead);
        }

        public virtual bool Get(string id, object sender, AssetRetrieved handler)
        {
            AssetBase asset = Get(id);
            handler(id, sender, asset);
            return true;
        }

        public virtual string Store(AssetBase asset)
        {
            return Store(asset, false);
        }

        public virtual string Store(AssetBase asset, bool force)
        {
            if (asset.FullID.IsZero())
            {
                if (string.IsNullOrEmpty(asset.ID) || !UUID.TryParse(asset.ID, out UUID id))
                    asset.FullID = UUID.Random();
                else
                    asset.FullID = id;
            }
            asset.ID = asset.FullID.ToString();

            if (asset.Data == null)
            {
                m_log.WarnFormat("[ADVANCED ASSET SERVICE]: Cannot store asset {0} with null data", asset.ID);
                return asset.ID;
            }

            m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Storing asset {0} (type {1}, name {2}, size {3})", 
                asset.ID, asset.Type, asset.Name, asset.Data.Length);

            m_PackManager.StoreAssetData(asset.ID, asset.Data, asset.Type, asset.Name);
            return asset.ID;
        }

        public virtual bool Delete(string id)
        {
            return false;
        }

        public virtual bool UpdateContent(string id, byte[] data)
        {
            AssetBase existing = Get(id);
            if (existing != null)
            {
                m_PackManager.StoreAssetData(id, data, existing.Type, existing.Name);
                return true;
            }
            return false;
        }

        public virtual bool[] AssetsExist(string[] ids)
        {
            bool[] exists = new bool[ids.Length];
            for (int i = 0; i < ids.Length; i++)
            {
                exists[i] = m_PackManager.AssetExists(ids[i]);
                if (!exists[i])
                    m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Asset check: {0} NOT FOUND in index", ids[i]);
            }
            return exists;
        }

        public void Get(string id, string ForeignAssetService, bool StoreOnLocalGrid, SimpleAssetRetrieved callBack)
        {
            callBack(Get(id));
        }

        // Command Handlers
        private void HandleImportLegacy(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas import-legacy <path>");
                return;
            }
            string path = args[2];
            if (!Directory.Exists(path))
            {
                MainConsole.Instance.Output("Directory not found.");
                return;
            }

            string[] files = Directory.GetFiles(path, "*.gz", SearchOption.AllDirectories);
            MainConsole.Instance.Output($"Found {files.Length} assets to import.");

            int count = 0;
            foreach (string file in files)
            {
                try
                {
                    string filename = Path.GetFileNameWithoutExtension(file);
                    sbyte type = (sbyte)AssetType.Unknown;
                    string assetID = filename;

                    // Handle format: UUID.type.gz or Hash.gz
                    if (filename.Contains("."))
                    {
                        string[] parts = filename.Split('.');
                        assetID = parts[0];
                        if (sbyte.TryParse(parts[1], out sbyte t))
                            type = t;
                    }

                    byte[] data;
                    using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read))
                    using (GZipStream gz = new GZipStream(fs, CompressionMode.Decompress))
                    using (MemoryStream ms = new MemoryStream())
                    {
                        gz.CopyTo(ms);
                        data = ms.ToArray();
                    }

                    // If it's a Hash (64 chars hex), we use it as ID if no UUID is present
                    // In a real FSAsset restoration, you'd want to restore the SQL DB too.
                    if (!UUID.TryParse(assetID, out UUID id))
                    {
                        // It's a hash or invalid ID. 
                        // To preserve links, the original UUID MUST be used.
                        // If we only have the hash, we use it as ID (OpenSim will handle it if the caller knows this ID)
                        m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Importing by content hash: {0}", assetID);
                    }

                    m_PackManager.StoreAssetData(assetID, data, type, "Legacy Import " + assetID);
                    count++;
                    if (count % 100 == 0) MainConsole.Instance.Output($"Imported {count}...");
                }
                catch (Exception ex)
                {
                    m_log.Error($"[ADVANCED ASSET SERVICE]: Error importing {file}: {ex.Message}");
                }
            }
            MainConsole.Instance.Output($"Total imported: {count}");
        }

        private void HandleImportCache(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas import-cache <path>");
                return;
            }
            string path = args[2];
            if (!Directory.Exists(path))
            {
                MainConsole.Instance.Output("Directory not found.");
                return;
            }

            string[] files = Directory.GetFiles(path, "*", SearchOption.AllDirectories);
            MainConsole.Instance.Output(string.Format("Found {0} files. Scanning for Flotsam cache assets...", files.Length));

            int totalScanned = 0;
            int successCount = 0;

            #pragma warning disable SYSLIB0011
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();

            foreach (string file in files)
            {
                string filename = Path.GetFileName(file);
                if (!UUID.TryParse(filename, out UUID assetID))
                {
                    continue;
                }

                totalScanned++;

                try
                {
                    using (FileStream fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        if (fs.Length == 0) continue;

                        AssetBase asset = (AssetBase)bformatter.Deserialize(fs);
                        if (asset != null)
                        {
                            Store(asset);
                            successCount++;
                            if (successCount % 100 == 0)
                            {
                                MainConsole.Instance.Output(string.Format("Imported {0} assets...", successCount));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    m_log.Debug(string.Format("[ADVANCED ASSET SERVICE]: Error importing cache file {0}: {1}", filename, ex.Message));
                }
            }
            #pragma warning restore SYSLIB0011

            MainConsole.Instance.Output(string.Format("Import scan finished. Scanned {0} UUID files. Successfully imported {1} assets to AdvancedAssetService.", totalScanned, successCount));
        }

        private void HandleExportLegacy(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas export-legacy <path>");
                return;
            }
            string basePath = args[2];
            if (!Directory.Exists(basePath)) Directory.CreateDirectory(basePath);

            var assets = m_PackManager.GetAllAssets();
            MainConsole.Instance.Output($"Exporting {assets.Count} assets to FSAsset compatible format (Hash.gz)...");

            int count = 0;
            HashSet<string> exportedHashes = new HashSet<string>();
            
            string sqlPath = Path.Combine(basePath, "metadata.sql");
            using (StreamWriter sw = new StreamWriter(sqlPath))
            {
                sw.WriteLine("-- AdvancedAssetService Metadata Export");
                sw.WriteLine("-- Use this to reconstruct the 'fsassets' or 'assets' table in MySQL/PostgreSQL");
                sw.WriteLine("");

                foreach (var meta in assets)
                {
                    try
                    {
                        // 1. Physical Export (Deduplicated)
                        if (!exportedHashes.Contains(meta.Hash))
                        {
                            byte[] data = m_PackManager.GetAssetData(meta.UUID, out _, out _);
                            if (data != null)
                            {
                                string relPath = HashToPath(meta.Hash);
                                string fullPath = Path.Combine(basePath, "data", relPath + ".gz");
                                Directory.CreateDirectory(Path.GetDirectoryName(fullPath));

                                using (FileStream fs = new FileStream(fullPath, FileMode.Create))
                                using (GZipStream gz = new GZipStream(fs, CompressionMode.Compress))
                                {
                                    gz.Write(data, 0, data.Length);
                                }
                                exportedHashes.Add(meta.Hash);
                            }
                        }

                        // 2. Metadata Export (SQL)
                        string safeName = meta.Name.Replace("'", "''");
                        sw.WriteLine($"INSERT INTO fsassets (id, hash, name, type, create_time) VALUES ('{meta.UUID}', '{meta.Hash}', '{safeName}', {meta.Type}, {meta.Created});");

                        count++;
                        if (count % 100 == 0) MainConsole.Instance.Output($"Processed {count} assets...");
                    }
                    catch (Exception ex)
                    {
                        m_log.Error($"[ADVANCED ASSET SERVICE]: Error exporting {meta.UUID}: {ex.Message}");
                    }
                }
            }
            
            MainConsole.Instance.Output($"Total processed: {count}");
            MainConsole.Instance.Output($"Deduplicated files: {exportedHashes.Count}");
            MainConsole.Instance.Output($"Metadata SQL saved to: {sqlPath}");
        }

        private string HashToPath(string hash)
        {
            if (hash == null || hash.Length < 10) return Path.Combine("junkyard", hash ?? "null");
            
            return Path.Combine(hash.Substring(0, 2),
                   Path.Combine(hash.Substring(2, 2),
                   Path.Combine(hash.Substring(4, 2),
                   hash)));
        }

        private void HandleExportAsset(string module, string[] args)
        {
            if (args.Length < 4)
            {
                MainConsole.Instance.Output("Syntax: aas export-asset <ID> <path>");
                return;
            }
            AssetBase asset = Get(args[2]);
            if (asset != null)
            {
                File.WriteAllBytes(args[3], asset.Data);
                MainConsole.Instance.Output($"Asset {args[2]} exported to {args[3]}");
            }
            else
            {
                MainConsole.Instance.Output("Asset not found");
            }
        }

        private void HandleImportAsset(string module, string[] args)
        {
            if (args.Length < 5)
            {
                MainConsole.Instance.Output("Syntax: aas import-asset <path> <type> <name>");
                return;
            }
            if (File.Exists(args[2]))
            {
                AssetBase asset = new AssetBase(UUID.Random(), args[4], (sbyte)int.Parse(args[3]), UUID.Zero.ToString());
                asset.Data = File.ReadAllBytes(args[2]);
                Store(asset);
                MainConsole.Instance.Output($"Asset imported with ID: {asset.ID}");
            }
        }

        private void HandleSearchContent(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas search-content <string>");
                return;
            }
            string pattern = args[2];
            MainConsole.Instance.Output($"Searching for '{pattern}' in asset metadata...");
            var results = m_PackManager.Search(pattern);
            foreach (var res in results)
            {
                MainConsole.Instance.Output(res);
            }
            MainConsole.Instance.Output($"Total found: {results.Count}");
        }

        private void HandleVerify(string module, string[] args)
        {
            m_PackManager.VerifyIntegrity(msg => MainConsole.Instance.Output(msg));
        }

        private void HandleRebuildIndex(string module, string[] args)
        {
            if (MainConsole.Instance.Prompt("This will delete the current index.db and recreate it from .bin files. Are you sure?", "no") != "yes")
            {
                MainConsole.Instance.Output("Aborted.");
                return;
            }
            m_PackManager.RebuildIndex();
        }

        private void ProcessShadowSync()
        {
            if (m_IsSyncing || m_GridConnector == null) return;
            m_IsSyncing = true;

            try
            {
                var unsynced = m_PackManager.GetUnsyncedAssets(100); // Batch of 100
                if (unsynced.Count > 0)
                {
                    m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Syncing {0} assets to grid database...", unsynced.Count);
                    int count = 0;
                    foreach (var meta in unsynced)
                    {
                        AssetMetadata am = new AssetMetadata
                        {
                            FullID = new UUID(meta.UUID),
                            ID = meta.UUID,
                            Type = meta.Type,
                            Name = meta.Name,
                            CreationDate = DateTimeOffset.FromUnixTimeSeconds(meta.Created).LocalDateTime
                        };

                        if (m_GridConnector.Store(am, meta.Hash))
                        {
                            m_PackManager.MarkAsSynced(meta.UUID);
                            count++;
                        }
                    }
                    if (count > 0) m_log.InfoFormat("[ADVANCED ASSET SERVICE]: Shadow Sync: {0} assets synchronized.", count);
                }
            }
            catch (Exception ex)
            {
                m_log.Error("[ADVANCED ASSET SERVICE]: Shadow Sync error: " + ex.Message);
            }
            finally
            {
                m_IsSyncing = false;
            }
        }

        public void Dispose()
        {
            m_SyncTimer?.Stop();
            m_S3Replicator?.Dispose();
            if (m_PackManager != null)
            {
                m_PackManager.Dispose();
                m_PackManager = null;
            }
        }
    }
}
