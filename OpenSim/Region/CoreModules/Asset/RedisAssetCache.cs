using System;
using System.Reflection;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using log4net;
using Nini.Config;
using Mono.Addins;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Services.Interfaces;
using StackExchange.Redis;

namespace OpenSim.Region.CoreModules.Asset
{
    [Extension(Path = "/OpenSim/RegionModules", NodeName = "RegionModule", Id = "RedisAssetCache")]
    public class RedisAssetCache : ISharedRegionModule, IAssetCache
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private bool m_Enabled = false;
        private ConnectionMultiplexer m_Redis = null;
        private IDatabase m_Db = null;
        private string m_ConnectionString = "localhost:6379";
        private int m_TtlSeconds = 86400; // 24 horas por padrão
        private bool m_UseCompression = true;

        // L1 Cache (Memória Local)
        private LruCache<string, AssetBase> m_L1Cache = null;
        private int m_L1CacheCapacity = 1000; // 1000 itens por padrão, 0 para desativar

        // Cache negativo local para evitar consultar o Redis/Robust excessivamente para chaves que não existem
        private readonly HashSet<string> m_NegativeCache = new HashSet<string>();

        public string Name => "RedisAssetCache";
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
                m_ConnectionString = cacheConfig.GetString("RedisConnectionString", m_ConnectionString);
                m_TtlSeconds = cacheConfig.GetInt("CacheTTL", m_TtlSeconds);
                m_UseCompression = cacheConfig.GetBoolean("UseCompression", m_UseCompression);
                m_L1CacheCapacity = cacheConfig.GetInt("L1CacheCapacity", m_L1CacheCapacity);
            }

            // Inicializa o cache L1 (Memória Local) se configurado
            if (m_L1CacheCapacity > 0)
            {
                m_L1Cache = new LruCache<string, AssetBase>(m_L1CacheCapacity);
                m_log.Info($"[REDIS ASSET CACHE]: L1 local memory cache enabled with capacity {m_L1CacheCapacity} items.");
            }

            try
            {
                m_log.Info($"[REDIS ASSET CACHE]: Connecting to Redis at {m_ConnectionString}...");
                
                // Configura opções de conexão com timeout e auto-reconnect
                var options = ConfigurationOptions.Parse(m_ConnectionString);
                options.ConnectTimeout = 1000;
                options.SyncTimeout = 1000;
                options.AbortOnConnectFail = false; // Não aborta para permitir reconexão automática em segundo plano
                
                m_Redis = ConnectionMultiplexer.Connect(options);
                m_Db = m_Redis.GetDatabase();
                
                m_log.Info($"[REDIS ASSET CACHE]: Redis connection initialized. TTL: {m_TtlSeconds}s, Compression: {m_UseCompression}");
            }
            catch (Exception ex)
            {
                m_log.Error($"[REDIS ASSET CACHE]: Failed to connect to Redis on startup: {ex.Message}. Running in L1-only fallback mode.");
                // Mantemos m_Enabled = true, pois rodaremos em modo degradado usando o L1 local e buscando do Robust
                m_Db = null;
            }
        }

        public void PostInitialise() {}
        
        public void Close()
        {
            if (m_Redis != null)
            {
                try
                {
                    m_Redis.Close();
                    m_Redis.Dispose();
                }
                catch {}
                m_Redis = null;
            }
            m_Db = null;
            m_L1Cache?.Clear();
        }

        public void AddRegion(Scene scene)
        {
            if (m_Enabled)
            {
                scene.RegisterModuleInterface<IAssetCache>(this);
            }
        }

        public void RemoveRegion(Scene scene)
        {
            if (m_Enabled)
            {
                scene.UnregisterModuleInterface<IAssetCache>(this);
            }
        }

        public void RegionLoaded(Scene scene) {}

        // IAssetCache Members

        public void Cache(AssetBase asset)
        {
            Cache(asset, false);
        }

        public void Cache(AssetBase asset, bool replace)
        {
            if (!m_Enabled || asset == null || string.IsNullOrEmpty(asset.ID)) return;

            // Remove do cache negativo caso tenha sido gravado agora
            lock (m_NegativeCache)
            {
                m_NegativeCache.Remove(asset.ID);
            }

            // Grava no cache L1 (Memória Local)
            if (m_L1Cache != null)
            {
                m_L1Cache.Add(asset.ID, asset);
            }

            // Grava no cache L2 (Redis)
            if (m_Db != null)
            {
                try
                {
                    byte[] serialized = SerializeAsset(asset);
                    byte[] payload = CompressPayload(serialized);

                    string key = $"opensim:asset:{asset.ID}";
                    m_Db.StringSet(key, payload, TimeSpan.FromSeconds(m_TtlSeconds));
                }
                catch (Exception ex)
                {
                    m_log.Warn($"[REDIS ASSET CACHE]: Failed to write asset {asset.ID} to Redis: {ex.Message}. Falling back to L1 cache only.");
                }
            }
        }

        public void CacheNegative(string id)
        {
            if (string.IsNullOrEmpty(id)) return;

            lock (m_NegativeCache)
            {
                m_NegativeCache.Add(id);
            }
        }

        public bool Get(string id, out AssetBase asset)
        {
            asset = null;
            if (!m_Enabled || string.IsNullOrEmpty(id)) return true;

            // Check negative cache
            lock (m_NegativeCache)
            {
                if (m_NegativeCache.Contains(id))
                {
                    return false;
                }
            }

            asset = GetCached(id);
            return true;
        }

        public AssetBase GetCached(string id)
        {
            if (!m_Enabled || string.IsNullOrEmpty(id)) return null;

            // 1. Tenta recuperar do cache L1 (Memória Local)
            if (m_L1Cache != null && m_L1Cache.TryGetValue(id, out AssetBase l1Asset))
            {
                return l1Asset;
            }

            // 2. Tenta recuperar do cache L2 (Redis)
            if (m_Db != null)
            {
                try
                {
                    string key = $"opensim:asset:{id}";
                    byte[] payload = m_Db.StringGet(key);
                    if (payload != null && payload.Length > 0)
                    {
                        byte[] decompressed = DecompressPayload(payload);
                        AssetBase asset = DeserializeAsset(id, decompressed);
                        
                        // Salva de volta no L1 para futuras requisições rápidas
                        if (m_L1Cache != null && asset != null)
                        {
                            m_L1Cache.Add(id, asset);
                        }
                        return asset;
                    }
                }
                catch (Exception ex)
                {
                    m_log.Warn($"[REDIS ASSET CACHE]: Failed to retrieve asset {id} from Redis: {ex.Message}. Falling back to Robust.");
                }
            }

            return null;
        }

        public bool GetFromMemory(string id, out AssetBase asset)
        {
            // Retorna do cache L1 se disponível
            if (m_L1Cache != null && m_L1Cache.TryGetValue(id, out asset))
            {
                return true;
            }
            asset = null;
            return false;
        }

        public bool Check(string id)
        {
            if (!m_Enabled || string.IsNullOrEmpty(id)) return false;

            // Se está no cache L1, existe
            if (m_L1Cache != null && m_L1Cache.Check(id))
            {
                return true;
            }

            // Caso contrário, checa no Redis
            if (m_Db != null)
            {
                try
                {
                    string key = $"opensim:asset:{id}";
                    return m_Db.KeyExists(key);
                }
                catch
                {
                    return false;
                }
            }

            return false;
        }

        public void Expire(string id)
        {
            if (!m_Enabled || string.IsNullOrEmpty(id)) return;

            lock (m_NegativeCache)
            {
                m_NegativeCache.Remove(id);
            }

            // Remove do L1
            m_L1Cache?.Remove(id);

            // Remove do Redis
            if (m_Db != null)
            {
                try
                {
                    string key = $"opensim:asset:{id}";
                    m_Db.KeyDelete(key);
                }
                catch (Exception ex)
                {
                    m_log.Warn($"[REDIS ASSET CACHE]: Failed to expire asset {id} from Redis: {ex.Message}");
                }
            }
        }

        public void Clear()
        {
            m_log.Info("[REDIS ASSET CACHE]: Clear requested. Clearing local L1 memory cache and negative cache.");
            
            lock (m_NegativeCache)
            {
                m_NegativeCache.Clear();
            }

            m_L1Cache?.Clear();
        }

        // Serialization and Compression Helpers

        private byte[] SerializeAsset(AssetBase asset)
        {
            using (MemoryStream ms = new MemoryStream())
            using (BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8))
            {
                bw.Write(asset.Name ?? string.Empty);
                bw.Write(asset.Description ?? string.Empty);
                bw.Write(asset.Type);
                bw.Write(asset.Local);
                bw.Write(asset.Temporary);
                bw.Write((int)asset.Flags);
                bw.Write(asset.Metadata.CreatorID ?? string.Empty);

                if (asset.Data != null)
                {
                    bw.Write(asset.Data.Length);
                    bw.Write(asset.Data);
                }
                else
                {
                    bw.Write(0);
                }

                bw.Flush();
                return ms.ToArray();
            }
        }

        private AssetBase DeserializeAsset(string id, byte[] bytes)
        {
            using (MemoryStream ms = new MemoryStream(bytes))
            using (BinaryReader br = new BinaryReader(ms, Encoding.UTF8))
            {
                string name = br.ReadString();
                string description = br.ReadString();
                sbyte type = br.ReadSByte();
                bool local = br.ReadBoolean();
                bool temporary = br.ReadBoolean();
                AssetFlags flags = (AssetFlags)br.ReadInt32();
                string creatorID = br.ReadString();

                int dataLen = br.ReadInt32();
                byte[] data = null;
                if (dataLen > 0)
                {
                    data = br.ReadBytes(dataLen);
                }

                AssetBase asset = new AssetBase(new UUID(id), name, type, creatorID);
                asset.Description = description;
                asset.Local = local;
                asset.Temporary = temporary;
                asset.Flags = flags;
                asset.Data = data;

                return asset;
            }
        }

        private byte[] CompressPayload(byte[] raw)
        {
            if (raw == null || raw.Length == 0)
                return raw;

            using (MemoryStream ms = new MemoryStream())
            {
                if (m_UseCompression)
                {
                    ms.WriteByte(1); // 1 = Deflate
                    using (DeflateStream ds = new DeflateStream(ms, CompressionLevel.Fastest, true))
                    {
                        ds.Write(raw, 0, raw.Length);
                    }
                }
                else
                {
                    ms.WriteByte(0); // 0 = Uncompressed
                    ms.Write(raw, 0, raw.Length);
                }
                return ms.ToArray();
            }
        }

        private byte[] DecompressPayload(byte[] compressed)
        {
            if (compressed == null || compressed.Length == 0)
                return compressed;

            // If first byte is 0, it is uncompressed
            if (compressed[0] == 0)
            {
                byte[] raw = new byte[compressed.Length - 1];
                Buffer.BlockCopy(compressed, 1, raw, 0, raw.Length);
                return raw;
            }

            // If first byte is 1, it is compressed with Deflate
            if (compressed[0] == 1)
            {
                using (MemoryStream msIn = new MemoryStream(compressed, 1, compressed.Length - 1))
                using (DeflateStream ds = new DeflateStream(msIn, CompressionMode.Decompress))
                using (MemoryStream msOut = new MemoryStream())
                {
                    ds.CopyTo(msOut);
                    return msOut.ToArray();
                }
            }

            // Fallback (older data, uncompressed)
            return compressed;
        }
    }

    /// <summary>
    /// Thread-safe LRU (Least Recently Used) cache implementation for L1.
    /// </summary>
    public class LruCache<TKey, TValue>
    {
        private readonly int m_Capacity;
        private readonly Dictionary<TKey, LinkedListNode<LruCacheItem>> m_Map;
        private readonly LinkedList<LruCacheItem> m_List;
        private readonly object m_Lock = new object();

        public LruCache(int capacity)
        {
            m_Capacity = capacity;
            m_Map = new Dictionary<TKey, LinkedListNode<LruCacheItem>>(capacity);
            m_List = new LinkedList<LruCacheItem>();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (m_Lock)
            {
                if (m_Map.TryGetValue(key, out LinkedListNode<LruCacheItem> node))
                {
                    value = node.Value.Value;
                    m_List.Remove(node);
                    m_List.AddFirst(node);
                    return true;
                }
                value = default;
                return false;
            }
        }

        public void Add(TKey key, TValue value)
        {
            lock (m_Lock)
            {
                if (m_Map.TryGetValue(key, out LinkedListNode<LruCacheItem> node))
                {
                    node.Value.Value = value;
                    m_List.Remove(node);
                    m_List.AddFirst(node);
                }
                else
                {
                    if (m_Map.Count >= m_Capacity && m_List.Last != null)
                    {
                        var last = m_List.Last;
                        m_Map.Remove(last.Value.Key);
                        m_List.RemoveLast();
                    }
                    var item = new LruCacheItem { Key = key, Value = value };
                    var newNode = new LinkedListNode<LruCacheItem>(item);
                    m_List.AddFirst(newNode);
                    m_Map[key] = newNode;
                }
            }
        }

        public void Remove(TKey key)
        {
            lock (m_Lock)
            {
                if (m_Map.TryGetValue(key, out LinkedListNode<LruCacheItem> node))
                {
                    m_Map.Remove(key);
                    m_List.Remove(node);
                }
            }
        }

        public bool Check(TKey key)
        {
            lock (m_Lock)
            {
                return m_Map.ContainsKey(key);
            }
        }

        public void Clear()
        {
            lock (m_Lock)
            {
                m_Map.Clear();
                m_List.Clear();
            }
        }

        private class LruCacheItem
        {
            public TKey Key;
            public TValue Value;
        }
    }
}
