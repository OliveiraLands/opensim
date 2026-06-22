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
        private int m_ShadowSyncBatchSize = 1000;

        protected string m_DatabaseProvider;
        protected string m_DatabaseConnectionString;

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
            m_ShadowSyncBatchSize = assetConfig.GetInt("ShadowSyncBatchSize", 1000);

            // Initialize Pack Manager
            m_PackManager = new PackFileManager(m_StoragePath);

            // Initialize Grid Connector (Shadow Sync)
            string dllName = assetConfig.GetString("StorageProvider", string.Empty);
            string connectionString = assetConfig.GetString("ConnectionString", string.Empty);
            string realm = assetConfig.GetString("Realm", "fsassets");

            m_DatabaseProvider = dllName;
            m_DatabaseConnectionString = connectionString;

            if (string.IsNullOrEmpty(m_DatabaseProvider) || string.IsNullOrEmpty(m_DatabaseConnectionString))
            {
                IConfig dbConfig = config.Configs["DatabaseService"];
                if (dbConfig != null)
                {
                    if (string.IsNullOrEmpty(m_DatabaseProvider))
                        m_DatabaseProvider = dbConfig.GetString("StorageProvider", string.Empty);
                    if (string.IsNullOrEmpty(m_DatabaseConnectionString))
                        m_DatabaseConnectionString = dbConfig.GetString("ConnectionString", string.Empty);
                }
            }

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
            m_S3Replicator = new S3BackgroundReplicator(assetConfig, m_StoragePath, m_PackManager);
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
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas sync-s3", "aas sync-s3", "Force synchronization of assets with S3", HandleSyncS3);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas sync-database", "aas sync-database", "Force full synchronization of assets with the grid database", HandleSyncDatabase);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas compare", "aas compare <path>", "Compare local AAS assets with an external asset folder", HandleCompare);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas import-raw", "aas import-raw <path>", "Bulk import raw uncompressed assets named by UUID", HandleImportRaw);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas scan-inventory", "aas scan-inventory <path>", "Scan inventory database and import missing assets from an external folder", HandleScanInventory);
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
            return m_PackManager.AssetsExist(ids);
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

            string[] files = SafeGetFiles(path).FindAll(f => f.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)).ToArray();
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
                        // m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Importing by content hash: {0}", assetID);
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

            string[] files = SafeGetFiles(path).ToArray();
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

        private void HandleSyncS3(string module, string[] args)
        {
            if (m_S3Replicator == null)
            {
                MainConsole.Instance.Output("S3 replication is not enabled or configured.");
                return;
            }

            System.Threading.Tasks.Task.Run(() =>
            {
                m_S3Replicator.ForceSync(msg => MainConsole.Instance.Output(msg));
            });
        }

        private void HandleCompare(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas compare <path>");
                return;
            }
            string path = args[2];
            if (!Directory.Exists(path))
            {
                MainConsole.Instance.Output("Directory not found: " + path);
                return;
            }

            MainConsole.Instance.Output("Scanning external folder for assets...");
            string[] files;
            try
            {
                files = SafeGetFiles(path).ToArray();
            }
            catch (Exception ex)
            {
                MainConsole.Instance.Output("Error scanning folder: " + ex.Message);
                return;
            }

            MainConsole.Instance.Output(string.Format("Found {0} files. Loading AAS asset records...", files.Length));
            
            var allAasAssets = m_PackManager.GetAllAssets();
            Dictionary<string, string> aasUuidToHash = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            HashSet<string> aasHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var record in allAasAssets)
            {
                aasUuidToHash[record.UUID] = record.Hash;
                aasHashes.Add(record.Hash);
            }

            MainConsole.Instance.Output(string.Format("AAS has {0} total asset UUIDs and {1} unique content hashes indexed.", aasUuidToHash.Count, aasHashes.Count));
            MainConsole.Instance.Output("Comparing assets...");

            int totalExternal = 0;
            int matched = 0;
            int mismatched = 0;
            int missingInAas = 0;
            int errorCount = 0;

            #pragma warning disable SYSLIB0011
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            #pragma warning restore SYSLIB0011

            foreach (string file in files)
            {
                // Skip index.db or pack files if the user points to an AAS storage path
                string filenameWithExt = Path.GetFileName(file);
                if (filenameWithExt == "index.db" || (filenameWithExt.StartsWith("pack_") && filenameWithExt.EndsWith(".bin")))
                {
                    continue;
                }

                totalExternal++;
                try
                {
                    string filename = Path.GetFileNameWithoutExtension(file);
                    string ext = Path.GetExtension(file).ToLower();

                    // Identify if the filename represents a UUID or a Hash
                    string assetID = filename;
                    if (filename.Contains("."))
                    {
                        string[] parts = filename.Split('.');
                        assetID = parts[0];
                    }

                    string normalizedID = assetID.ToLower().Replace("-", "");
                    bool isHash = normalizedID.Length == 64; // SHA256 hex is 64 chars

                    byte[] extData = null;
                    if (ext == ".gz")
                    {
                        using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read))
                        using (GZipStream gz = new GZipStream(fs, CompressionMode.Decompress))
                        using (MemoryStream ms = new MemoryStream())
                        {
                            gz.CopyTo(ms);
                            extData = ms.ToArray();
                        }
                    }
                    else
                    {
                        byte[] fileBytes = File.ReadAllBytes(file);
                        // Check if it's Flotsam BinaryFormatter serialized cache file
                        bool isFlotsam = false;
                        if (fileBytes.Length > 9 && fileBytes[0] == 0x00 && fileBytes[1] == 0x01 && fileBytes[2] == 0x00 && fileBytes[3] == 0x00 && fileBytes[4] == 0x00 && fileBytes[5] == 0xff && fileBytes[6] == 0xff && fileBytes[7] == 0xff && fileBytes[8] == 0xff)
                        {
                            isFlotsam = true;
                        }

                        if (isFlotsam)
                        {
                            #pragma warning disable SYSLIB0011
                            using (MemoryStream ms = new MemoryStream(fileBytes))
                            {
                                AssetBase asset = (AssetBase)bformatter.Deserialize(ms);
                                extData = asset?.Data;
                            }
                            #pragma warning restore SYSLIB0011
                        }
                        else
                        {
                            extData = fileBytes;
                        }
                    }

                    if (extData == null)
                    {
                        MainConsole.Instance.Output(string.Format("[ERROR] Could not read asset data from {0}", filenameWithExt));
                        errorCount++;
                        continue;
                    }

                    // Compute SHA256 hash of external asset data
                    string extHash;
                    using (System.Security.Cryptography.SHA256 sha = System.Security.Cryptography.SHA256.Create())
                    {
                        extHash = BitConverter.ToString(sha.ComputeHash(extData)).Replace("-", "").ToLower();
                    }

                    if (isHash)
                    {
                        // Deduplicated file by Hash
                        if (aasHashes.Contains(normalizedID))
                        {
                            if (normalizedID == extHash)
                            {
                                matched++;
                            }
                            else
                            {
                                mismatched++;
                                MainConsole.Instance.Output(string.Format("[MISMATCH] Hash file {0} has content hash {1} instead of {2}", filenameWithExt, extHash, normalizedID));
                            }
                        }
                        else
                        {
                            missingInAas++;
                            MainConsole.Instance.Output(string.Format("[MISSING] AAS is missing data for hash {0} (found in {1})", normalizedID, filenameWithExt));
                        }
                    }
                    else
                    {
                        // File by UUID
                        if (aasUuidToHash.TryGetValue(normalizedID, out string aasHash))
                        {
                            if (aasHash == extHash)
                            {
                                matched++;
                            }
                            else
                            {
                                mismatched++;
                                MainConsole.Instance.Output(string.Format("[CORRUPTED/MISMATCH] UUID {0} ({1}) has content hash {2} in external, but AAS records hash {3}", normalizedID, filenameWithExt, extHash, aasHash));
                            }
                        }
                        else
                        {
                            if (aasHashes.Contains(extHash))
                            {
                                missingInAas++;
                                MainConsole.Instance.Output(string.Format("[MISSING LINK] UUID {0} ({1}) not in AAS, but its content hash {2} exists in AAS.", normalizedID, filenameWithExt, extHash));
                            }
                            else
                            {
                                missingInAas++;
                                MainConsole.Instance.Output(string.Format("[MISSING] UUID {0} ({1}) and its content hash {2} are completely missing in AAS.", normalizedID, filenameWithExt, extHash));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    MainConsole.Instance.Output(string.Format("[ERROR] Failed to compare file {0}: {1}", filenameWithExt, ex.Message));
                    errorCount++;
                }
            }

            MainConsole.Instance.Output("--- Comparison Summary ---");
            MainConsole.Instance.Output(string.Format("Total Files Checked:       {0}", totalExternal));
            MainConsole.Instance.Output(string.Format("Identical Match (Valid):    {0}", matched));
            MainConsole.Instance.Output(string.Format("Content Mismatch/Corrupt:  {0}", mismatched));
            MainConsole.Instance.Output(string.Format("Missing in AAS:            {0}", missingInAas));
            MainConsole.Instance.Output(string.Format("Errors processing files:   {0}", errorCount));
        }

        private void HandleImportRaw(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas import-raw <path>");
                return;
            }
            string path = args[2];
            if (!Directory.Exists(path))
            {
                MainConsole.Instance.Output("Directory not found: " + path);
                return;
            }

            string[] files;
            try
            {
                files = SafeGetFiles(path).ToArray();
            }
            catch (Exception ex)
            {
                MainConsole.Instance.Output("Error scanning folder: " + ex.Message);
                return;
            }

            MainConsole.Instance.Output(string.Format("Found {0} files to scan.", files.Length));

            int count = 0;
            int skipped = 0;
            foreach (string file in files)
            {
                try
                {
                    string filename = Path.GetFileName(file);
                    
                    // Skip system files like index.db, or pack files
                    if (filename == "index.db" || (filename.StartsWith("pack_") && filename.EndsWith(".bin")))
                    {
                        continue;
                    }

                    sbyte type = (sbyte)AssetType.Unknown;
                    string assetID = filename;

                    if (filename.Contains("."))
                    {
                        string[] parts = filename.Split('.');
                        assetID = parts[0];
                        if (parts.Length > 1 && sbyte.TryParse(parts[1], out sbyte t))
                            type = t;
                    }

                    string normalizedID = assetID.ToLower().Replace("-", "");
                    string name = "Raw Import " + normalizedID;

                    if (!UUID.TryParse(normalizedID, out UUID id))
                    {
                        skipped++;
                        continue;
                    }

                    // Query the grid database (MySQL) to fetch original metadata if available
                    if (m_GridConnector != null)
                    {
                        try
                        {
                            string existingHash;
                            AssetMetadata dbMeta = m_GridConnector.Get(id.ToString(), out existingHash);
                            if (dbMeta != null)
                            {
                                type = dbMeta.Type;
                                name = dbMeta.Name;
                            }
                        }
                        catch (Exception ex)
                        {
                            m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Failed to query grid database for metadata of {0}: {1}", id, ex.Message);
                        }
                    }

                    byte[] data = File.ReadAllBytes(file);
                    if (data == null || data.Length == 0)
                    {
                        skipped++;
                        continue;
                    }

                    // StoreAssetData expects the UUID format (normalized or formatted, Store handles both)
                    m_PackManager.StoreAssetData(id.ToString(), data, type, name);
                    count++;
                    if (count % 100 == 0) MainConsole.Instance.Output(string.Format("Imported {0} raw assets...", count));
                }
                catch (Exception ex)
                {
                    m_log.Error(string.Format("[ADVANCED ASSET SERVICE]: Error importing raw asset {0}: {1}", file, ex.Message));
                }
            }
            MainConsole.Instance.Output(string.Format("Total raw assets imported: {0} (skipped/non-UUID: {1})", count, skipped));
        }

        private void HandleScanInventory(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: aas scan-inventory <path>");
                return;
            }
            string path = args[2];
            if (!Directory.Exists(path))
            {
                MainConsole.Instance.Output("Directory not found: " + path);
                return;
            }

            if (string.IsNullOrEmpty(m_DatabaseProvider) || string.IsNullOrEmpty(m_DatabaseConnectionString))
            {
                MainConsole.Instance.Output("Database provider or connection string not configured.");
                return;
            }

            MainConsole.Instance.Output("Loading inventory database plugin...");
            IXInventoryData invDatabase;
            try
            {
                invDatabase = LoadPlugin<IXInventoryData>(m_DatabaseProvider, new object[] { m_DatabaseConnectionString, string.Empty });
            }
            catch (Exception ex)
            {
                MainConsole.Instance.Output("Failed to load inventory database: " + ex.Message);
                return;
            }

            if (invDatabase == null)
            {
                MainConsole.Instance.Output("Failed to instantiate inventory database plugin.");
                return;
            }

            MainConsole.Instance.Output("Scanning inventory items from database...");
            XInventoryItem[] items;
            try
            {
                items = invDatabase.GetItems(new string[0], new string[0]);
            }
            catch (Exception ex)
            {
                MainConsole.Instance.Output("Failed to query inventory items: " + ex.Message);
                return;
            }

            if (items == null || items.Length == 0)
            {
                MainConsole.Instance.Output("No items found in inventory.");
                return;
            }

            MainConsole.Instance.Output(string.Format("Found {0} inventory items. Checking for missing assets in AAS...", items.Length));

            HashSet<UUID> inventoryAssetIDs = new HashSet<UUID>();
            Dictionary<UUID, XInventoryItem> itemMetadata = new Dictionary<UUID, XInventoryItem>();

            foreach (var item in items)
            {
                if (item.assetID != UUID.Zero)
                {
                    inventoryAssetIDs.Add(item.assetID);
                    if (!itemMetadata.ContainsKey(item.assetID))
                    {
                        itemMetadata[item.assetID] = item;
                    }
                }
            }

            List<UUID> missingIDs = new List<UUID>();
            foreach (UUID assetID in inventoryAssetIDs)
            {
                if (!m_PackManager.AssetExists(assetID.ToString()))
                {
                    missingIDs.Add(assetID);
                }
            }

            MainConsole.Instance.Output(string.Format("Total unique assets in inventory: {0}", inventoryAssetIDs.Count));
            MainConsole.Instance.Output(string.Format("Assets missing in AAS:           {0}", missingIDs.Count));

            if (missingIDs.Count == 0)
            {
                MainConsole.Instance.Output("No missing assets to import. AAS is fully up-to-date with inventory.");
                return;
            }

            MainConsole.Instance.Output("Scanning external folder for asset files...");
            string[] allFiles;
            try
            {
                allFiles = SafeGetFiles(path).ToArray();
            }
            catch (Exception ex)
            {
                MainConsole.Instance.Output("Error scanning folder: " + ex.Message);
                return;
            }

            MainConsole.Instance.Output(string.Format("Found {0} files in external folder. Indexing files...", allFiles.Length));
            
            Dictionary<string, string> filesByName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (string file in allFiles)
            {
                string filename = Path.GetFileName(file);
                string normName = filename.ToLower().Replace("-", "");
                filesByName[normName] = file;

                if (filename.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                {
                    string withoutGz = filename.Substring(0, filename.Length - 3);
                    string normWithoutGz = withoutGz.ToLower().Replace("-", "");
                    filesByName[normWithoutGz] = file;

                    if (withoutGz.Contains("."))
                    {
                        string uuidPart = withoutGz.Split('.')[0];
                        string normUuidPart = uuidPart.ToLower().Replace("-", "");
                        filesByName[normUuidPart] = file;
                    }
                }

                string withoutExt = Path.GetFileNameWithoutExtension(file);
                string normWithoutExt = withoutExt.ToLower().Replace("-", "");
                filesByName[normWithoutExt] = file;
                if (withoutExt.Contains("."))
                {
                    string uuidPart = withoutExt.Split('.')[0];
                    string normUuidPart = uuidPart.ToLower().Replace("-", "");
                    filesByName[normUuidPart] = file;
                }
            }

            MainConsole.Instance.Output("Searching and importing missing assets...");

            int importedCount = 0;
            int notFoundCount = 0;
            int errorCount = 0;

            #pragma warning disable SYSLIB0011
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            #pragma warning restore SYSLIB0011

            foreach (UUID assetID in missingIDs)
            {
                try
                {
                    string normUuid = assetID.ToString().ToLower().Replace("-", "");
                    string matchedFilePath = null;

                    // 1. Try to fetch metadata and content hash from the MySQL/grid database
                    string dbHash = null;
                    sbyte type = (sbyte)AssetType.Unknown;
                    string name = null;

                    if (m_GridConnector != null)
                    {
                        try
                        {
                            string existingHash;
                            AssetMetadata dbMeta = m_GridConnector.Get(assetID.ToString(), out existingHash);
                            if (dbMeta != null)
                            {
                                type = dbMeta.Type;
                                name = dbMeta.Name;
                                dbHash = existingHash;
                            }
                        }
                        catch { }
                    }

                    // Fallback to inventory item metadata if grid database doesn't have it
                    if (name == null)
                    {
                        if (itemMetadata.TryGetValue(assetID, out XInventoryItem item))
                        {
                            type = (sbyte)item.assetType;
                            name = item.inventoryName;
                        }
                        else
                        {
                            name = "Restored Inventory Asset " + normUuid;
                        }
                    }

                    // 2. Locate file in the indexed external files
                    // First search by UUID (normalized)
                    if (filesByName.TryGetValue(normUuid, out string fileByUuid))
                    {
                        matchedFilePath = fileByUuid;
                    }
                    // If content hash was found, search by Hash
                    else if (!string.IsNullOrEmpty(dbHash))
                    {
                        string normHash = dbHash.ToLower().Replace("-", "");
                        if (filesByName.TryGetValue(normHash, out string fileByHash))
                        {
                            matchedFilePath = fileByHash;
                        }
                    }

                    if (string.IsNullOrEmpty(matchedFilePath))
                    {
                        notFoundCount++;
                        continue;
                    }

                    // 3. Read and import the file
                    byte[] extData = null;
                    string ext = Path.GetExtension(matchedFilePath).ToLower();

                    if (ext == ".gz")
                    {
                        using (FileStream fs = new FileStream(matchedFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                        using (GZipStream gz = new GZipStream(fs, CompressionMode.Decompress))
                        using (MemoryStream ms = new MemoryStream())
                        {
                            gz.CopyTo(ms);
                            extData = ms.ToArray();
                        }
                    }
                    else
                    {
                        byte[] fileBytes = File.ReadAllBytes(matchedFilePath);
                        // Check if it is Flotsam BinaryFormatter serialized cache file
                        bool isFlotsam = false;
                        if (fileBytes.Length > 9 && fileBytes[0] == 0x00 && fileBytes[1] == 0x01 && fileBytes[2] == 0x00 && fileBytes[3] == 0x00 && fileBytes[4] == 0x00 && fileBytes[5] == 0xff && fileBytes[6] == 0xff && fileBytes[7] == 0xff && fileBytes[8] == 0xff)
                        {
                            isFlotsam = true;
                        }

                        if (isFlotsam)
                        {
                            #pragma warning disable SYSLIB0011
                            using (MemoryStream ms = new MemoryStream(fileBytes))
                            {
                                AssetBase asset = (AssetBase)bformatter.Deserialize(ms);
                                extData = asset?.Data;
                            }
                            #pragma warning restore SYSLIB0011
                        }
                        else
                        {
                            extData = fileBytes;
                        }
                    }

                    if (extData == null || extData.Length == 0)
                    {
                        errorCount++;
                        continue;
                    }

                    m_PackManager.StoreAssetData(assetID.ToString(), extData, type, name);
                    importedCount++;
                }
                catch (Exception ex)
                {
                    m_log.Error(string.Format("[ADVANCED ASSET SERVICE]: Error restoring inventory asset {0}: {1}", assetID, ex.Message));
                    errorCount++;
                }
            }

            MainConsole.Instance.Output("--- Inventory Scan & Import Summary ---");
            MainConsole.Instance.Output(string.Format("Assets Missing in AAS:   {0}", missingIDs.Count));
            MainConsole.Instance.Output(string.Format("Successfully Restored:   {0}", importedCount));
            MainConsole.Instance.Output(string.Format("Not Found in External:   {0}", notFoundCount));
            MainConsole.Instance.Output(string.Format("Errors during Import:    {0}", errorCount));
        }

        private void HandleSyncDatabase(string module, string[] args)
        {
            if (m_GridConnector == null)
            {
                MainConsole.Instance.Output("Database synchronization (Shadow Sync) is not enabled.");
                return;
            }

            if (m_IsSyncing)
            {
                MainConsole.Instance.Output("Synchronization is already in progress.");
                return;
            }

            m_IsSyncing = true;
            try
            {
                MainConsole.Instance.Output("Starting full database synchronization...");
                int totalSynced = 0;
                int batchSize = m_ShadowSyncBatchSize;
                
                while (true)
                {
                    var unsynced = m_PackManager.GetUnsyncedAssets(batchSize);
                    if (unsynced.Count == 0)
                        break;

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

                        try
                        {
                            if (m_GridConnector.Store(am, meta.Hash))
                            {
                                m_PackManager.MarkAsSynced(meta.UUID);
                                count++;
                            }
                        }
                        catch (Exception ex)
                        {
                            m_log.Error("[ADVANCED ASSET SERVICE]: Error syncing asset: " + ex.Message);
                        }
                    }

                    totalSynced += count;
                    MainConsole.Instance.Output(string.Format("Synchronized {0} assets in this batch (Total: {1})...", count, totalSynced));

                    if (count < unsynced.Count)
                    {
                        MainConsole.Instance.Output("Some assets failed to synchronize. Stopping full synchronization to avoid infinite loop.");
                        break;
                    }
                }

                MainConsole.Instance.Output(string.Format("Full database synchronization finished. Total assets synchronized: {0}", totalSynced));
            }
            finally
            {
                m_IsSyncing = false;
            }
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
                if (m_log.IsDebugEnabled)
                {
                    var unsynced = m_PackManager.GetUnsyncedAssets(m_ShadowSyncBatchSize);
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
                else
                {
                    var unsynced = m_PackManager.GetUnsyncedAssets(m_ShadowSyncBatchSize);
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
                            m_PackManager.MarkAsSynced(meta.UUID);
                    }
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

        private List<string> SafeGetFiles(string path)
        {
            List<string> files = new List<string>();
            Queue<string> queue = new Queue<string>();
            queue.Enqueue(path);
            while (queue.Count > 0)
            {
                string currentDir = queue.Dequeue();
                try
                {
                    foreach (string file in Directory.GetFiles(currentDir))
                    {
                        files.Add(file);
                    }
                    foreach (string subDir in Directory.GetDirectories(currentDir))
                    {
                        queue.Enqueue(subDir);
                    }
                }
                catch (UnauthorizedAccessException)
                {
                    // Ignore permission errors and continue
                }
                catch (Exception)
                {
                    // Ignore other errors (e.g. broken symlinks) and continue
                }
            }
            return files;
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
