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
using MySql.Data.MySqlClient;
using System.Data.SQLite;

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
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas defrag", "aas defrag", "Defragment PackFiles and release dead storage space", HandleDefragment);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas deep-repair", "aas deep-repair", "Deep scan PackFiles byte-by-byte and salvage active records", HandleDeepRepair);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas audit-grid", "aas audit-grid [--repair]", "Audit grid metadata consistency against AAS database", HandleAuditGrid);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas repair-links", "aas repair-links", "Repair broken links pointing to missing assets using fallback data", HandleRepairLinks);
            MainConsole.Instance.Commands.AddCommand("aas", false, "aas scan-used-assets", "aas scan-used-assets <db_mask> [<import_folder>] [--flag-suspicious]", "Scan inventories and region databases to identify used assets, importing missing ones and optionally flagging unused ones as suspicious", HandleScanUsedAssets);
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

            int startIndex = m_PackManager.PromptResumeProgress("import-legacy", path, files.Length, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("import-legacy", path, files.Length);
            }

            int count = 0;
            for (int i = startIndex; i < files.Length; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    MainConsole.Instance.Output("Legacy import aborted by user.");
                    return;
                }
                string file = files[i];
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

                    // Validate UUID. If it's a hash (e.g. 64 chars long) or invalid UUID, try to lookup in grid database
                    if (!UUID.TryParse(assetID, out UUID id))
                    {
                        string foundUUID = null;
                        if (m_GridConnector != null)
                        {
                            try
                            {
                                foundUUID = m_GridConnector.GetUUIDByHash(assetID);
                            }
                            catch (Exception ex)
                            {
                                m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Failed to query grid database for UUID of hash {0}: {1}", assetID, ex.Message);
                            }
                        }

                        if (!string.IsNullOrEmpty(foundUUID) && UUID.TryParse(foundUUID, out id))
                        {
                            m_log.InfoFormat("[ADVANCED ASSET SERVICE]: Resolved hash '{0}' to UUID '{1}' via grid database", assetID, id);
                        }
                        else
                        {
                            m_log.WarnFormat("[ADVANCED ASSET SERVICE]: Skipping legacy import for '{0}' (not a valid UUID and not found in grid database)", filename);
                            m_PackManager.UpdateCommandProgress("import-legacy", i + 1);
                            continue;
                        }
                    }
                    assetID = id.ToString();

                    m_PackManager.StoreAssetData(assetID, data, type, "Legacy Import " + assetID);
                    count++;
                    if (count % 100 == 0) MainConsole.Instance.Output($"Imported {count}...");
                }
                catch (Exception ex)
                {
                    m_log.Error($"[ADVANCED ASSET SERVICE]: Error importing {file}: {ex.Message}");
                }
                m_PackManager.UpdateCommandProgress("import-legacy", i + 1);
            }
            m_PackManager.ClearCommandProgress("import-legacy");
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

            List<string> cacheFiles = new List<string>();
            foreach (string file in files)
            {
                if (UUID.TryParse(Path.GetFileName(file), out _))
                {
                    cacheFiles.Add(file);
                }
            }

            int startIndex = m_PackManager.PromptResumeProgress("import-cache", path, cacheFiles.Count, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("import-cache", path, cacheFiles.Count);
            }

            int successCount = 0;

            #pragma warning disable SYSLIB0011
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();

            for (int i = startIndex; i < cacheFiles.Count; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    MainConsole.Instance.Output("Cache import aborted by user.");
                    return;
                }
                string file = cacheFiles[i];
                string filename = Path.GetFileName(file);

                try
                {
                    using (FileStream fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        if (fs.Length == 0)
                        {
                            m_PackManager.UpdateCommandProgress("import-cache", i + 1);
                            continue;
                        }

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
                m_PackManager.UpdateCommandProgress("import-cache", i + 1);
            }
            #pragma warning restore SYSLIB0011

            m_PackManager.ClearCommandProgress("import-cache");
            MainConsole.Instance.Output(string.Format("Import scan finished. Scanned {0} UUID files. Successfully imported {1} assets to AdvancedAssetService.", cacheFiles.Count, successCount));
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

            List<string> compareFiles = new List<string>();
            foreach (string file in files)
            {
                string filenameWithExt = Path.GetFileName(file);
                if (filenameWithExt == "index.db" || (filenameWithExt.StartsWith("pack_") && filenameWithExt.EndsWith(".bin")))
                {
                    continue;
                }
                compareFiles.Add(file);
            }

            int matched = 0;
            int mismatched = 0;
            int missingInAas = 0;
            int errorCount = 0;

            int startIndex = m_PackManager.PromptResumeProgress("compare", path, compareFiles.Count, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("compare", path, compareFiles.Count);
            }
            else
            {
                string metadata = m_PackManager.GetConfig("cmd_state:compare:metadata");
                if (!string.IsNullOrEmpty(metadata))
                {
                    string[] parts = metadata.Split(',');
                    if (parts.Length == 4)
                    {
                        int.TryParse(parts[0], out matched);
                        int.TryParse(parts[1], out mismatched);
                        int.TryParse(parts[2], out missingInAas);
                        int.TryParse(parts[3], out errorCount);
                    }
                }
            }

            #pragma warning disable SYSLIB0011
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            #pragma warning restore SYSLIB0011

            for (int i = startIndex; i < compareFiles.Count; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    MainConsole.Instance.Output("Comparison aborted by user.");
                    return;
                }
                string file = compareFiles[i];
                string filenameWithExt = Path.GetFileName(file);

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
                        m_PackManager.UpdateCommandProgress("compare", i + 1);
                        m_PackManager.SetConfig("cmd_state:compare:metadata", string.Format("{0},{1},{2},{3}", matched, mismatched, missingInAas, errorCount));
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
                m_PackManager.UpdateCommandProgress("compare", i + 1);
                m_PackManager.SetConfig("cmd_state:compare:metadata", string.Format("{0},{1},{2},{3}", matched, mismatched, missingInAas, errorCount));
            }

            m_PackManager.ClearCommandProgress("compare");

            MainConsole.Instance.Output("--- Comparison Summary ---");
            MainConsole.Instance.Output(string.Format("Total Files Checked:       {0}", compareFiles.Count));
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

            List<string> rawFiles = new List<string>();
            foreach (string file in files)
            {
                string filename = Path.GetFileName(file);
                
                // Skip system files like index.db, or pack files
                if (filename == "index.db" || (filename.StartsWith("pack_") && filename.EndsWith(".bin")))
                {
                    continue;
                }

                string assetID = filename;

                if (filename.Contains("."))
                {
                    string[] parts = filename.Split('.');
                    assetID = parts[0];
                }

                string normalizedID = assetID.ToLower().Replace("-", "");

                if (UUID.TryParse(normalizedID, out _))
                {
                    rawFiles.Add(file);
                }
            }

            int startIndex = m_PackManager.PromptResumeProgress("import-raw", path, rawFiles.Count, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("import-raw", path, rawFiles.Count);
            }

            int count = 0;
            int skipped = 0;
            for (int i = startIndex; i < rawFiles.Count; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    MainConsole.Instance.Output("Raw import aborted by user.");
                    return;
                }
                string file = rawFiles[i];
                try
                {
                    string filename = Path.GetFileName(file);
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
                    UUID id = new UUID(normalizedID);

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
                        m_PackManager.UpdateCommandProgress("import-raw", i + 1);
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
                m_PackManager.UpdateCommandProgress("import-raw", i + 1);
            }
            m_PackManager.ClearCommandProgress("import-raw");
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

            int startIndex = m_PackManager.PromptResumeProgress("scan-inventory", path, missingIDs.Count, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("scan-inventory", path, missingIDs.Count);
            }
            else
            {
                string metadata = m_PackManager.GetConfig("cmd_state:scan-inventory:metadata");
                if (!string.IsNullOrEmpty(metadata))
                {
                    string[] parts = metadata.Split(',');
                    if (parts.Length == 3)
                    {
                        int.TryParse(parts[0], out importedCount);
                        int.TryParse(parts[1], out notFoundCount);
                        int.TryParse(parts[2], out errorCount);
                    }
                }
            }

            #pragma warning disable SYSLIB0011
            System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            #pragma warning restore SYSLIB0011

            for (int i = startIndex; i < missingIDs.Count; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    MainConsole.Instance.Output("Inventory scan aborted by user.");
                    return;
                }
                UUID assetID = missingIDs[i];
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
                        m_PackManager.UpdateCommandProgress("scan-inventory", i + 1);
                        m_PackManager.SetConfig("cmd_state:scan-inventory:metadata", string.Format("{0},{1},{2}", importedCount, notFoundCount, errorCount));
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
                        m_PackManager.UpdateCommandProgress("scan-inventory", i + 1);
                        m_PackManager.SetConfig("cmd_state:scan-inventory:metadata", string.Format("{0},{1},{2}", importedCount, notFoundCount, errorCount));
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
                m_PackManager.UpdateCommandProgress("scan-inventory", i + 1);
                m_PackManager.SetConfig("cmd_state:scan-inventory:metadata", string.Format("{0},{1},{2}", importedCount, notFoundCount, errorCount));
            }

            m_PackManager.ClearCommandProgress("scan-inventory");
            MainConsole.Instance.Output(string.Format("Errors during Import:    {0}", errorCount));
        }

        private void HandleDefragment(string module, string[] args)
        {
            if (MainConsole.Instance.Prompt("This will rewrite all PackFiles and rebuild the index. Are you sure?", "no") != "yes")
            {
                MainConsole.Instance.Output("Aborted.");
                return;
            }
            m_PackManager.Defragment(m_GridConnector, msg => MainConsole.Instance.Output("AAS Defrag: " + msg));
        }

        private void HandleDeepRepair(string module, string[] args)
        {
            if (MainConsole.Instance.Prompt("This will delete the current index and salvage active records byte-by-byte from PackFiles. Are you sure?", "no") != "yes")
            {
                MainConsole.Instance.Output("Aborted.");
                return;
            }
            m_PackManager.RebuildIndexResilient(msg => MainConsole.Instance.Output("AAS Repair: " + msg));
        }

        private void HandleScanUsedAssets(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Usage: aas scan-used-assets <db_mask> [<import_folder>] [--flag-suspicious] (e.g. 'os_%')");
                return;
            }

            string dbMask = args[2];
            string importFolder = null;
            bool flagSuspicious = false;

            for (int i = 3; i < args.Length; i++)
            {
                if (args[i].Equals("--flag-suspicious", StringComparison.OrdinalIgnoreCase) || args[i].Equals("true", StringComparison.OrdinalIgnoreCase))
                {
                    flagSuspicious = true;
                }
                else if (args[i].Equals("--no-flag-suspicious", StringComparison.OrdinalIgnoreCase) || args[i].Equals("false", StringComparison.OrdinalIgnoreCase))
                {
                    flagSuspicious = false;
                }
                else
                {
                    importFolder = args[i];
                }
            }

            if (string.IsNullOrEmpty(m_DatabaseProvider) || string.IsNullOrEmpty(m_DatabaseConnectionString))
            {
                MainConsole.Instance.Output("Grid database is not configured.");
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

            HashSet<string> usedUuids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> uuidSources = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            int invCount = 0;
            if (items != null)
            {
                foreach (var item in items)
                {
                    if (item.assetID != UUID.Zero)
                    {
                        string nid = item.assetID.ToString().ToLower().Replace("-", "");
                        if (usedUuids.Add(nid))
                        {
                            uuidSources[nid] = "Inventory";
                            invCount++;
                        }
                    }
                }
            }
            MainConsole.Instance.Output(string.Format("Found {0} used asset references in inventory.", invCount));

            if (m_DatabaseProvider.Contains("MySQL") || m_DatabaseProvider.Contains("MySql"))
            {
                MainConsole.Instance.Output("Connecting to MySQL to scan region databases matching mask: " + dbMask);
                try
                {
                    using (MySqlConnection myConn = new MySqlConnection(m_DatabaseConnectionString))
                    {
                        myConn.Open();

                        // 1. Get matching databases
                        List<string> databases = new List<string>();
                        using (MySqlCommand cmd = myConn.CreateCommand())
                        {
                            cmd.CommandText = "SHOW DATABASES LIKE @mask";
                            cmd.Parameters.AddWithValue("@mask", dbMask);
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    databases.Add(reader.GetString(0));
                                }
                            }
                        }

                        MainConsole.Instance.Output(string.Format("Found {0} matching region databases on the server.", databases.Count));

                        foreach (string db in databases)
                        {
                            MainConsole.Instance.Output(string.Format("Scanning database '{0}'...", db));
                            try
                            {
                                // 2. Check which tables exist in this database
                                HashSet<string> existingTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                using (MySqlCommand cmd = myConn.CreateCommand())
                                {
                                    cmd.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @db";
                                    cmd.Parameters.AddWithValue("@db", db);
                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            existingTables.Add(reader.GetString(0));
                                        }
                                    }
                                }

                                // 3. Scan primitems
                                if (existingTables.Contains("primitems"))
                                {
                                    int itemsCount = 0;
                                    using (MySqlCommand cmd = myConn.CreateCommand())
                                    {
                                        cmd.CommandText = string.Format("SELECT DISTINCT assetID FROM `{0}`.primitems WHERE assetID IS NOT NULL AND assetID != '00000000-0000-0000-0000-000000000000'", db);
                                        using (var reader = cmd.ExecuteReader())
                                        {
                                            while (reader.Read())
                                            {
                                                string rawId = reader.GetValue(0)?.ToString();
                                                if (UUID.TryParse(rawId, out UUID uuid) && uuid != UUID.Zero)
                                                {
                                                    string nid = uuid.ToString().ToLower().Replace("-", "");
                                                    if (usedUuids.Add(nid))
                                                    {
                                                        uuidSources[nid] = string.Format("{0}.primitems", db);
                                                        itemsCount++;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    MainConsole.Instance.Output(string.Format("  Found {0} used assets in {1}.primitems", itemsCount, db));
                                }

                                // 4. Scan primshapes
                                if (existingTables.Contains("primshapes"))
                                {
                                    int texturesCount = 0;
                                    List<byte[]> textureBlobs = new List<byte[]>();
                                    using (MySqlCommand cmd = myConn.CreateCommand())
                                    {
                                        cmd.CommandText = string.Format("SELECT Texture FROM `{0}`.primshapes WHERE Texture IS NOT NULL", db);
                                        using (var reader = cmd.ExecuteReader())
                                        {
                                            while (reader.Read())
                                            {
                                                if (!reader.IsDBNull(0))
                                                {
                                                    textureBlobs.Add((byte[])reader.GetValue(0));
                                                }
                                            }
                                        }
                                    }

                                    foreach (var blob in textureBlobs)
                                    {
                                        try
                                        {
                                            OpenMetaverse.Primitive.TextureEntry te = new OpenMetaverse.Primitive.TextureEntry(blob, 0, blob.Length);
                                            if (te != null)
                                            {
                                                if (te.DefaultTexture != null && te.DefaultTexture.TextureID != UUID.Zero)
                                                {
                                                    string nid = te.DefaultTexture.TextureID.ToString().ToLower().Replace("-", "");
                                                    if (usedUuids.Add(nid))
                                                    {
                                                        uuidSources[nid] = string.Format("{0}.primshapes (DefaultTexture)", db);
                                                        texturesCount++;
                                                    }
                                                }
                                                if (te.FaceTextures != null)
                                                {
                                                    foreach (var face in te.FaceTextures)
                                                    {
                                                        if (face != null && face.TextureID != UUID.Zero)
                                                        {
                                                            string nid = face.TextureID.ToString().ToLower().Replace("-", "");
                                                            if (usedUuids.Add(nid))
                                                            {
                                                                uuidSources[nid] = string.Format("{0}.primshapes (FaceTexture)", db);
                                                                texturesCount++;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        catch {}
                                    }

                                    // 4b. Check other columns in primshapes (normal_map_texture, specular_map_texture)
                                    List<string> extraCols = new List<string>();
                                    using (MySqlCommand cmd = myConn.CreateCommand())
                                    {
                                        cmd.CommandText = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @db AND TABLE_NAME = 'primshapes' AND COLUMN_NAME IN ('normal_map_texture', 'specular_map_texture')";
                                        cmd.Parameters.AddWithValue("@db", db);
                                        using (var reader = cmd.ExecuteReader())
                                        {
                                            while (reader.Read())
                                            {
                                                extraCols.Add(reader.GetString(0));
                                            }
                                        }
                                    }

                                    foreach (var col in extraCols)
                                    {
                                        int colCount = 0;
                                        using (MySqlCommand cmd = myConn.CreateCommand())
                                        {
                                            cmd.CommandText = string.Format("SELECT DISTINCT `{0}` FROM `{1}`.primshapes WHERE `{0}` IS NOT NULL AND `{0}` != '' AND `{0}` != '00000000-0000-0000-0000-000000000000'", col, db);
                                            using (var reader = cmd.ExecuteReader())
                                            {
                                                while (reader.Read())
                                                {
                                                    string rawId = reader.GetValue(0)?.ToString();
                                                    if (UUID.TryParse(rawId, out UUID uuid) && uuid != UUID.Zero)
                                                    {
                                                        string nid = uuid.ToString().ToLower().Replace("-", "");
                                                        if (usedUuids.Add(nid))
                                                        {
                                                            uuidSources[nid] = string.Format("{0}.primshapes ({1})", db, col);
                                                            colCount++;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        MainConsole.Instance.Output(string.Format("  Found {0} used assets in {1}.primshapes ({2})", colCount, db, col));
                                    }

                                    MainConsole.Instance.Output(string.Format("  Found {0} used texture assets in {1}.primshapes (Texture Entry)", texturesCount, db));
                                }

                                // 5. Scan regionsettings
                                if (existingTables.Contains("regionsettings"))
                                {
                                    List<string> rsCols = new List<string>();
                                    using (MySqlCommand cmd = myConn.CreateCommand())
                                    {
                                        cmd.CommandText = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @db AND TABLE_NAME = 'regionsettings' AND (COLUMN_NAME LIKE '%texture%' OR COLUMN_NAME LIKE '%Terrain%')";
                                        cmd.Parameters.AddWithValue("@db", db);
                                        using (var reader = cmd.ExecuteReader())
                                        {
                                            while (reader.Read())
                                            {
                                                rsCols.Add(reader.GetString(0));
                                            }
                                        }
                                    }

                                    foreach (var col in rsCols)
                                    {
                                        int colCount = 0;
                                        using (MySqlCommand cmd = myConn.CreateCommand())
                                        {
                                            cmd.CommandText = string.Format("SELECT DISTINCT `{0}` FROM `{1}`.regionsettings WHERE `{0}` IS NOT NULL AND `{0}` != '' AND `{0}` != '00000000-0000-0000-0000-000000000000'", col, db);
                                            using (var reader = cmd.ExecuteReader())
                                            {
                                                while (reader.Read())
                                                {
                                                    string rawId = reader.GetValue(0)?.ToString();
                                                    if (UUID.TryParse(rawId, out UUID uuid) && uuid != UUID.Zero)
                                                    {
                                                        string nid = uuid.ToString().ToLower().Replace("-", "");
                                                        if (usedUuids.Add(nid))
                                                        {
                                                            uuidSources[nid] = string.Format("{0}.regionsettings ({1})", db, col);
                                                            colCount++;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if (colCount > 0)
                                        {
                                            MainConsole.Instance.Output(string.Format("  Found {0} used assets in {1}.regionsettings ({2})", colCount, db, col));
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                MainConsole.Instance.Output(string.Format("Error scanning database '{0}': {1}", db, ex.Message));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    MainConsole.Instance.Output("MySQL scan error: " + ex.Message);
                }
            }
            else
            {
                MainConsole.Instance.Output("Database provider is not MySQL/MariaDB. Database matching skipped.");
            }

            if (!string.IsNullOrEmpty(importFolder))
            {
                if (!Directory.Exists(importFolder))
                {
                    MainConsole.Instance.Output("Import folder not found: " + importFolder);
                }
                else
                {
                    MainConsole.Instance.Output("Scanning import folder recursively for missing used assets...");
                    Dictionary<string, string> importableFiles = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        foreach (string file in Directory.EnumerateFiles(importFolder, "*", SearchOption.AllDirectories))
                        {
                            string filename = Path.GetFileName(file);
                            if (filename == "index.db" || (filename.StartsWith("pack_") && filename.EndsWith(".bin")))
                                continue;

                            string key = Path.GetFileNameWithoutExtension(filename).ToLower().Replace("-", "");
                            if (key.Contains("."))
                            {
                                key = key.Split('.')[0];
                            }
                            importableFiles[key] = file;
                        }
                        MainConsole.Instance.Output(string.Format("Found {0} files in import folder.", importableFiles.Count));
                    }
                    catch (Exception ex)
                    {
                        MainConsole.Instance.Output("Error scanning import folder: " + ex.Message);
                    }

                    int importSuccess = 0;
                    int importFailed = 0;

                    foreach (var nid in usedUuids)
                    {
                        if (!m_PackManager.AssetExists(nid))
                        {
                            if (importableFiles.TryGetValue(nid, out string filePath))
                            {
                                string formattedUuid = new UUID(nid).ToString();
                                if (TryImportAssetFromFile(filePath, formattedUuid, out string error))
                                {
                                    importSuccess++;
                                }
                                else
                                {
                                    importFailed++;
                                    m_log.DebugFormat("[ADVANCED ASSET SERVICE]: Failed to import missing used asset {0} from {1}: {2}", formattedUuid, filePath, error);
                                }
                            }
                        }
                    }

                    if (importSuccess > 0 || importFailed > 0)
                    {
                        MainConsole.Instance.Output(string.Format("Import completed: {0} assets imported, {1} failed.", importSuccess, importFailed));
                    }
                    else
                    {
                        MainConsole.Instance.Output("No missing used assets were found in the import folder.");
                    }
                }
            }

            // Write to SQLite base of used assets
            MainConsole.Instance.Output("Saving used assets to SQLite database: used_assets.db...");
            string usedDbPath = Path.Combine(m_StoragePath, "used_assets.db");
            if (File.Exists(usedDbPath))
            {
                try { File.Delete(usedDbPath); } catch {}
            }

            try
            {
                using (SQLiteConnection SQLiteConn = new SQLiteConnection(string.Format("Data Source={0};Version=3;", usedDbPath)))
                {
                    SQLiteConn.Open();
                    using (SQLiteCommand cmd = SQLiteConn.CreateCommand())
                    {
                        cmd.CommandText = "CREATE TABLE used_assets (uuid TEXT PRIMARY KEY, source TEXT)";
                        cmd.ExecuteNonQuery();
                    }

                    using (SQLiteTransaction trans = SQLiteConn.BeginTransaction())
                    {
                        using (SQLiteCommand cmd = SQLiteConn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT OR IGNORE INTO used_assets (uuid, source) VALUES (?, ?)";
                            var p1 = cmd.CreateParameter();
                            var p2 = cmd.CreateParameter();
                            cmd.Parameters.Add(p1);
                            cmd.Parameters.Add(p2);

                            foreach (var nid in usedUuids)
                            {
                                p1.Value = nid;
                                p2.Value = uuidSources[nid];
                                cmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                MainConsole.Instance.Output("Error creating used_assets.db: " + ex.Message);
            }

            MainConsole.Instance.Output("Evaluating used assets presence in AdvancedAssetService (AAS)...");
            int foundInAas = 0;
            int missingInAas = 0;

            foreach (var nid in usedUuids)
            {
                if (m_PackManager.AssetExists(nid))
                {
                    foundInAas++;
                }
                else
                {
                    missingInAas++;
                }
            }

            MainConsole.Instance.Output(string.Format("Scan Result:"));
            MainConsole.Instance.Output(string.Format("  Total unique used assets: {0}", usedUuids.Count));
            MainConsole.Instance.Output(string.Format("  Present in AAS:           {0}", foundInAas));
            MainConsole.Instance.Output(string.Format("  Missing in AAS:           {0}", missingInAas));

            // Mark unused ones in AAS as suspicious if requested
            if (flagSuspicious)
            {
                MainConsole.Instance.Output("Identifying unused assets in AAS to flag as suspicious...");
                List<AssetMetadataRecord> allAasAssets = m_PackManager.GetAllAssets();
                List<string> unusedUuids = new List<string>();

                foreach (var meta in allAasAssets)
                {
                    if (!usedUuids.Contains(meta.UUID))
                    {
                        unusedUuids.Add(meta.UUID);
                    }
                }

                MainConsole.Instance.Output(string.Format("Found {0} unused assets in AAS. Flagging them as suspicious...", unusedUuids.Count));
                m_PackManager.SetSuspiciousAssets(unusedUuids);
                MainConsole.Instance.Output("Scan completed successfully. Unused assets flagged as suspicious and will be cleaned up in the next defrag run.");
            }
            else
            {
                MainConsole.Instance.Output("Scan completed successfully. Flagging unused assets as suspicious is disabled (use '--flag-suspicious' or 'true' to enable).");
            }
        }

        private void HandleAuditGrid(string module, string[] args)
        {
            if (m_GridConnector == null)
            {
                MainConsole.Instance.Output("Database synchronization (Shadow Sync) is not enabled.");
                return;
            }

            MainConsole.Instance.Output("Starting Grid vs AAS database audit...");
            var allAssets = m_PackManager.GetAllAssets();
            
            int total = allAssets.Count;
            int missingInGrid = 0;
            int hashMismatch = 0;
            int syncedInGrid = 0;
            int errorCount = 0;
            bool repair = (args.Length > 2 && args[2] == "--repair");
            string key = repair ? "repair" : "audit";

            int startIndex = m_PackManager.PromptResumeProgress("audit-grid", key, total, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("audit-grid", key, total);
            }
            else
            {
                string metadata = m_PackManager.GetConfig("cmd_state:audit-grid:metadata");
                if (!string.IsNullOrEmpty(metadata))
                {
                    string[] parts = metadata.Split(',');
                    if (parts.Length == 4)
                    {
                        int.TryParse(parts[0], out missingInGrid);
                        int.TryParse(parts[1], out hashMismatch);
                        int.TryParse(parts[2], out syncedInGrid);
                        int.TryParse(parts[3], out errorCount);
                    }
                }
            }

            MainConsole.Instance.Output(string.Format("Auditing {0} local assets against grid database...", total));

            for (int i = startIndex; i < total; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    m_PackManager.UpdateCommandProgress("audit-grid", i);
                    m_PackManager.SetConfig("cmd_state:audit-grid:metadata", string.Format("{0},{1},{2},{3}", missingInGrid, hashMismatch, syncedInGrid, errorCount));
                    MainConsole.Instance.Output("Grid audit aborted by user.");
                    return;
                }
                var meta = allAssets[i];
                if ((i + 1) % 1000 == 0 || i + 1 == total)
                {
                    MainConsole.Instance.Output(string.Format("Audited {0} / {1}...", i + 1, total));
                    m_PackManager.UpdateCommandProgress("audit-grid", i + 1);
                    m_PackManager.SetConfig("cmd_state:audit-grid:metadata", string.Format("{0},{1},{2},{3}", missingInGrid, hashMismatch, syncedInGrid, errorCount));
                }

                try
                {
                    string gridHash;
                    AssetMetadata gridMeta = m_GridConnector.Get(meta.UUID, out gridHash);

                    if (gridMeta == null)
                    {
                        missingInGrid++;
                        MainConsole.Instance.Output(string.Format("[MISSING IN GRID] UUID {0} is in AAS but missing in MySQL.", meta.UUID));
                        
                        if (repair)
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
                                MainConsole.Instance.Output(string.Format(" -> Repaired: Uploaded UUID {0} to MySQL.", meta.UUID));
                            }
                        }
                    }
                    else if (gridHash != meta.Hash)
                    {
                        hashMismatch++;
                        MainConsole.Instance.Output(string.Format("[HASH MISMATCH] UUID {0} has local hash {1} but grid hash {2}.", meta.UUID, meta.Hash, gridHash));
                        
                        if (repair)
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
                                MainConsole.Instance.Output(string.Format(" -> Repaired: Updated UUID {0} hash in MySQL.", meta.UUID));
                            }
                        }
                    }
                    else
                    {
                        syncedInGrid++;
                    }
                }
                catch (Exception ex)
                {
                    MainConsole.Instance.Output(string.Format("[ERROR] Failed to audit asset {0}: {1}", meta.UUID, ex.Message));
                    errorCount++;
                }
            }

            m_PackManager.ClearCommandProgress("audit-grid");

            MainConsole.Instance.Output("--- Audit Summary ---");
            MainConsole.Instance.Output(string.Format("Total local assets:   {0}", total));
            MainConsole.Instance.Output(string.Format("Synced in Grid:       {0}", syncedInGrid));
            MainConsole.Instance.Output(string.Format("Missing in Grid:      {0}", missingInGrid));
            MainConsole.Instance.Output(string.Format("Hash Mismatch:        {0}", hashMismatch));
            if (errorCount > 0)
            {
                MainConsole.Instance.Output(string.Format("Audit Errors:         {0}", errorCount));
            }
            if (!repair && (missingInGrid > 0 || hashMismatch > 0))
            {
                MainConsole.Instance.Output("Run 'aas audit-grid --repair' to automatically push missing/corrected metadata to grid database.");
            }
        }

        private void HandleRepairLinks(string module, string[] args)
        {
            MainConsole.Instance.Output("Scanning for broken asset links...");
            var broken = m_PackManager.GetBrokenLinks();
            if (broken.Count == 0)
            {
                MainConsole.Instance.Output("No broken links found. AAS is healthy.");
                return;
            }

            MainConsole.Instance.Output(string.Format("Found {0} broken asset links. Attempting recovery...", broken.Count));
            int recovered = 0;
            int defaultFallback = 0;

            int startIndex = m_PackManager.PromptResumeProgress("repair-links", "run", broken.Count, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("repair-links", "run", broken.Count);
            }
            else
            {
                string metadata = m_PackManager.GetConfig("cmd_state:repair-links:metadata");
                if (!string.IsNullOrEmpty(metadata))
                {
                    string[] parts = metadata.Split(',');
                    if (parts.Length == 2)
                    {
                        int.TryParse(parts[0], out recovered);
                        int.TryParse(parts[1], out defaultFallback);
                    }
                }
            }

            for (int i = startIndex; i < broken.Count; i++)
            {
                if (m_PackManager.CheckUserAbort())
                {
                    MainConsole.Instance.Output("Repair links aborted by user.");
                    return;
                }
                var kvp = broken[i];
                string uuid = kvp.Key;
                byte[] data = null;
                sbyte type = (sbyte)AssetType.Unknown;
                string name = "Recovered Fallback Asset";

                if (m_GridConnector != null)
                {
                    try
                    {
                        string dummy;
                        AssetMetadata dbMeta = m_GridConnector.Get(uuid, out dummy);
                        if (dbMeta != null)
                        {
                            type = dbMeta.Type;
                            name = dbMeta.Name;
                        }
                    }
                    catch {}
                }

                if (data == null)
                {
                    defaultFallback++;
                    data = GenerateFallbackData(type);
                    name = string.IsNullOrEmpty(name) ? "Fallback Asset" : name;
                }

                if (data != null)
                {
                    m_PackManager.StoreAssetData(uuid, data, type, name);
                    recovered++;
                    MainConsole.Instance.Output(string.Format(" -> Restored UUID {0} (Type: {1}, Name: '{2}') with fallback data.", uuid, type, name));
                }

                m_PackManager.UpdateCommandProgress("repair-links", i + 1);
                m_PackManager.SetConfig("cmd_state:repair-links:metadata", string.Format("{0},{1}", recovered, defaultFallback));
            }

            m_PackManager.ClearCommandProgress("repair-links");

            MainConsole.Instance.Output("--- Recovery Summary ---");
            MainConsole.Instance.Output(string.Format("Broken links processed: {0}", broken.Count));
            MainConsole.Instance.Output(string.Format("Successfully repaired:  {0} (Default fallbacks used: {1})", recovered, defaultFallback));
        }

        private byte[] GenerateFallbackData(sbyte type)
        {
            switch ((AssetType)type)
            {
                case AssetType.Texture:
                    // 1x1 transparent PNG
                    return new byte[] {
                        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
                        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
                        0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0xDA, 0x63, 0x60, 0x60, 0x60, 0x60,
                        0x00, 0x00, 0x00, 0x05, 0x00, 0x01, 0xA5, 0x67, 0xB9, 0xCF, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
                        0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82
                    };
                case AssetType.Sound:
                    // 0.1s silent sound data (Ogg Vorbis minimal header)
                    return new byte[] {
                        0x4f, 0x67, 0x67, 0x53, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xbc, 0xd0, 0x1d, 0x1e, 0x01, 0x1e, 0x01, 0x76, 0x6f, 0x72,
                        0x62, 0x69, 0x73, 0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xac, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb8, 0x01
                    };
                default:
                    return new byte[] { 0 };
            }
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
                    if (m_PackManager.CheckUserAbort())
                    {
                        MainConsole.Instance.Output("Database synchronization aborted by user.");
                        break;
                    }
                    var unsynced = m_PackManager.GetUnsyncedAssets(batchSize);
                    if (unsynced.Count == 0)
                        break;

                    int count = 0;
                    foreach (var meta in unsynced)
                    {
                        if (meta.UUID.Length > 36)
                        {
                            m_log.WarnFormat("[ADVANCED ASSET SERVICE]: Asset UUID '{0}' is too long ({1} chars) for grid database. Marking as synced to prevent error loop.", meta.UUID, meta.UUID.Length);
                            m_PackManager.MarkAsSynced(meta.UUID);
                            count++;
                            continue;
                        }

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

            int startIndex = m_PackManager.PromptResumeProgress("export-legacy", basePath, assets.Count, out bool resume);
            if (!resume)
            {
                m_PackManager.StartCommandProgress("export-legacy", basePath, assets.Count);
            }
            else
            {
                string dataDir = Path.Combine(basePath, "data");
                if (Directory.Exists(dataDir))
                {
                    try
                    {
                        foreach (string file in SafeGetFiles(dataDir))
                        {
                            exportedHashes.Add(Path.GetFileNameWithoutExtension(file));
                        }
                    }
                    catch {}
                }
            }

            string sqlPath = Path.Combine(basePath, "metadata.sql");
            using (StreamWriter sw = new StreamWriter(sqlPath, resume))
            {
                if (!resume)
                {
                    sw.WriteLine("-- AdvancedAssetService Metadata Export");
                    sw.WriteLine("-- Use this to reconstruct the 'fsassets' or 'assets' table in MySQL/PostgreSQL");
                    sw.WriteLine("");
                }

                for (int i = startIndex; i < assets.Count; i++)
                {
                    if (m_PackManager.CheckUserAbort())
                    {
                        MainConsole.Instance.Output("Legacy export aborted by user.");
                        return;
                    }
                    var meta = assets[i];
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
                    m_PackManager.UpdateCommandProgress("export-legacy", i + 1);
                }
            }

            m_PackManager.ClearCommandProgress("export-legacy");
            
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
                            if (meta.UUID.Length > 36)
                            {
                                m_log.WarnFormat("[ADVANCED ASSET SERVICE]: Asset UUID '{0}' is too long ({1} chars) for grid database. Marking as synced to prevent error loop.", meta.UUID, meta.UUID.Length);
                                m_PackManager.MarkAsSynced(meta.UUID);
                                count++;
                                continue;
                            }

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
                        if (meta.UUID.Length > 36)
                        {
                            m_log.WarnFormat("[ADVANCED ASSET SERVICE]: Asset UUID '{0}' is too long ({1} chars) for grid database. Marking as synced to prevent error loop.", meta.UUID, meta.UUID.Length);
                            m_PackManager.MarkAsSynced(meta.UUID);
                            continue;
                        }

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

        private bool TryImportAssetFromFile(string filePath, string uuidStr, out string error)
        {
            error = string.Empty;
            try
            {
                string filename = Path.GetFileName(filePath);
                string ext = Path.GetExtension(filePath).ToLower();
                sbyte type = (sbyte)AssetType.Unknown;
                string name = "Imported " + uuidStr;

                string nameWithoutExt = Path.GetFileNameWithoutExtension(filePath);
                if (nameWithoutExt.Contains("."))
                {
                    string[] parts = nameWithoutExt.Split('.');
                    if (parts.Length > 1 && sbyte.TryParse(parts[1], out sbyte t))
                    {
                        type = t;
                    }
                }

                byte[] data = null;

                if (ext == ".gz")
                {
                    using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                    using (GZipStream gz = new GZipStream(fs, CompressionMode.Decompress))
                    using (MemoryStream ms = new MemoryStream())
                    {
                        gz.CopyTo(ms);
                        data = ms.ToArray();
                    }
                }
                else
                {
                    byte[] fileBytes = File.ReadAllBytes(filePath);
                    bool isFlotsam = fileBytes.Length > 9 && 
                                     fileBytes[0] == 0x00 && fileBytes[1] == 0x01 && 
                                     fileBytes[2] == 0x00 && fileBytes[3] == 0x00 && 
                                     fileBytes[4] == 0x00 && fileBytes[5] == 0xff && 
                                     fileBytes[6] == 0xff && fileBytes[7] == 0xff && 
                                     fileBytes[8] == 0xff;

                    if (isFlotsam)
                    {
#pragma warning disable SYSLIB0011
                        var bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                        using (MemoryStream ms = new MemoryStream(fileBytes))
                        {
                            AssetBase asset = (AssetBase)bformatter.Deserialize(ms);
                            if (asset != null)
                            {
                                Store(asset);
                                return true;
                            }
                        }
#pragma warning restore SYSLIB0011
                    }
                    else
                    {
                        data = fileBytes;
                    }
                }

                if (data == null || data.Length == 0)
                {
                    error = "Empty or invalid data";
                    return false;
                }

                if (m_GridConnector != null)
                {
                    try
                    {
                        string existingHash;
                        AssetMetadata dbMeta = m_GridConnector.Get(uuidStr, out existingHash);
                        if (dbMeta != null)
                        {
                            type = dbMeta.Type;
                            name = dbMeta.Name;
                        }
                    }
                    catch {}
                }

                m_PackManager.StoreAssetData(uuidStr, data, type, name);
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
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
