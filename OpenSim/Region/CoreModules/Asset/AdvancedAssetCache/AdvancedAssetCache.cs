using System;
using System.Reflection;
using System.Collections.Generic;
using log4net;
using Nini.Config;
using Mono.Addins;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Services.Interfaces;

namespace OpenSim.Region.CoreModules.Asset
{
    [Extension(Path = "/OpenSim/RegionModules", NodeName = "RegionModule", Id = "AdvancedAssetCache")]
    public class AdvancedAssetCache : ISharedRegionModule, IAssetCache
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private bool m_Enabled = false;
        private string m_CacheDirectory = "assetcache_aac";
        private long m_MaxCacheSize = 2048L * 1024 * 1024; // 2GB
        private long m_MaxPackSize = 256L * 1024 * 1024; // 256MB

        private PackFileCache m_PackCache;

        public string Name => "AdvancedAssetCache";
        public Type ReplaceableInterface => typeof(IAssetCache);

        public void Initialise(IConfigSource source)
        {
            IConfig moduleConfig = source.Configs["Modules"];
            if (moduleConfig == null || moduleConfig.GetString("AssetCaching", string.Empty) != Name)
                return;

            m_Enabled = true;
            IConfig cacheConfig = source.Configs["AssetCache"];
            if (cacheConfig != null)
            {
                m_CacheDirectory = cacheConfig.GetString("CacheDirectory", m_CacheDirectory);
                m_MaxCacheSize = cacheConfig.GetLong("MaxCacheSize", 2048) * 1024 * 1024;
                m_MaxPackSize = cacheConfig.GetLong("MaxPackSize", 256) * 1024 * 1024;
            }

            m_PackCache = new PackFileCache(m_CacheDirectory, m_MaxCacheSize, m_MaxPackSize);
            m_log.Info($"[ADVANCED ASSET CACHE]: Initialized with directory {m_CacheDirectory}, max size {m_MaxCacheSize / 1024 / 1024}MB, max pack size {m_MaxPackSize / 1024 / 1024}MB");
        }

        public void PostInitialise() {}
        public void Close() { m_PackCache?.Dispose(); }
        public void AddRegion(Scene scene) { if (m_Enabled) scene.RegisterModuleInterface<IAssetCache>(this); }
        public void RemoveRegion(Scene scene) { if (m_Enabled) scene.UnregisterModuleInterface<IAssetCache>(this); }
        public void RegionLoaded(Scene scene) {}

        public void Cache(AssetBase asset)
        {
            Cache(asset, false);
        }

        public void Cache(AssetBase asset, bool replace)
        {
            if (asset == null || string.IsNullOrEmpty(asset.ID) || asset.Data == null) return;
            m_PackCache.Store(asset.ID, asset.Data, asset.Type, asset.Name);
        }

        public void CacheNegative(string id)
        {
            // We can implement a simple negative cache in memory if needed.
            // For now, AAC focus is on positive disk cache.
        }

        public bool Get(string id, out AssetBase asset)
        {
            asset = Get(id);
            return asset != null;
        }

        public AssetBase Get(string id)
        {
            if (string.IsNullOrEmpty(id)) return null;

            sbyte type;
            string name;
            byte[] data = m_PackCache.Get(id, out type, out name);

            if (data != null)
            {
                AssetBase asset = new AssetBase(new UUID(id), name, type, UUID.Zero.ToString());
                asset.Data = data;
                return asset;
            }
            return null;
        }

        public AssetBase GetCached(string id)
        {
            return Get(id);
        }

        public bool GetFromMemory(string id, out AssetBase asset)
        {
            asset = null;
            return false;
        }

        public bool Check(string id)
        {
            return Get(id) != null;
        }

        public void Expire(string id)
        {
            // With PackFiles, individual expiration is handled by rotation and LRU in PackFileCache.
            // We could implement individual delete in the index if needed.
        }

        public void Clear()
        {
            m_PackCache?.Clear();
        }
    }
}
