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

namespace OpenSim.Services.AdvancedAssetService
{
    public class AdvancedAssetService : ServiceBase, IAssetService, IDisposable
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string m_StoragePath;
        protected IAssetService m_FallbackService;
        protected bool m_VerifyOnRead = true;
        
        private PackFileManager m_PackManager;

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

            // Setup Fallback Service
            string fallback = assetConfig.GetString("FallbackService", string.Empty);
            if (!string.IsNullOrEmpty(fallback))
            {
                m_FallbackService = LoadPlugin<IAssetService>(fallback, new object[] { config });
            }

            RegisterCommands();
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
        }

        public virtual AssetBase Get(string id)
        {
            sbyte type;
            string name;
            byte[] data = m_PackManager.GetAssetData(id, out type, out name);
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
            return m_PackManager.GetAssetData(id, out type, out name);
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
            MainConsole.Instance.Output($"Found {files.Length} legacy assets to import.");

            int count = 0;
            foreach (string file in files)
            {
                try
                {
                    string hash = Path.GetFileNameWithoutExtension(file);
                    byte[] data;
                    using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read))
                    using (GZipStream gz = new GZipStream(fs, CompressionMode.Decompress))
                    using (MemoryStream ms = new MemoryStream())
                    {
                        gz.CopyTo(ms);
                        data = ms.ToArray();
                    }

                    // For legacy import without DB, we use Hash as Name and generate a UUID if hash is not a UUID
                    string assetID = hash;
                    if (!UUID.TryParse(hash, out UUID id))
                        assetID = UUID.Random().ToString();

                    m_PackManager.StoreAssetData(assetID, data, (sbyte)AssetType.Unknown, "Legacy Import " + hash);
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
            MainConsole.Instance.Output($"Exporting {assets.Count} assets to legacy format...");

            int count = 0;
            foreach (var meta in assets)
            {
                try
                {
                    byte[] data = m_PackManager.GetAssetData(meta.UUID, out _, out _);
                    if (data == null) continue;

                    string relPath = HashToPath(meta.Hash);
                    string fullPath = Path.Combine(basePath, relPath);
                    Directory.CreateDirectory(Path.GetDirectoryName(fullPath));

                    using (FileStream fs = new FileStream(fullPath + ".gz", FileMode.Create))
                    using (GZipStream gz = new GZipStream(fs, CompressionMode.Compress))
                    {
                        gz.Write(data, 0, data.Length);
                    }
                    count++;
                    if (count % 100 == 0) MainConsole.Instance.Output($"Exported {count}...");
                }
                catch (Exception ex)
                {
                    m_log.Error($"[ADVANCED ASSET SERVICE]: Error exporting {meta.UUID}: {ex.Message}");
                }
            }
            MainConsole.Instance.Output($"Total exported: {count}");
        }

        private string HashToPath(string hash)
        {
            if (hash == null || hash.Length < 10) return Path.Combine("junkyard", hash ?? "null");
            
            string path = Path.Combine(hash.Substring(0, 2),
                          Path.Combine(hash.Substring(2, 2),
                          Path.Combine(hash.Substring(4, 2),
                          hash.Substring(6, 4))));
            
            return Path.Combine(path, hash);
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

        public void Dispose()
        {
            if (m_PackManager != null)
            {
                m_PackManager.Dispose();
                m_PackManager = null;
            }
        }
    }
}
