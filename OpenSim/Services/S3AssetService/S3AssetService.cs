/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * Veja CONTRIBUTORS.TXT para uma lista completa de detentores de direitos autorais.
 *
 * (Licença BSD omitida para brevidade, mas mantida como no original)
 */

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using StackExchange.Redis;
using log4net;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Data;
using OpenSim.Framework;
using OpenSim.Framework.Serialization.External;
using OpenSim.Services.Base;
using OpenSim.Services.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;

namespace OpenSim.Services.S3AssetService
{
    public class S3AssetConnector : ServiceBase, IAssetService
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected IAssetLoader m_AssetLoader = null;
        protected IAssetDataPlugin m_DataConnector = null;
        protected IAssetService m_FallbackService;
        protected IAmazonS3 s3Client;
        protected string s3Bucket;

        private ConnectionMultiplexer redis;
        private IDatabase cacheDb;

        private static bool m_mainInitialized;
        private static object m_initLock = new object();

        private bool m_isMainInstance;

        private IConfig assetConfig;

        public S3AssetConnector(IConfigSource config)
            : this(config, "AssetService")
        {
        }

        public S3AssetConnector(IConfigSource config, string configName) : base(config)
        {
            assetConfig = config.Configs[configName];
            if (assetConfig == null)
                throw new Exception("No AssetService configuration");

            lock (m_initLock)
            {
                if (!m_mainInitialized)
                {
                    m_mainInitialized = true;
                    m_isMainInstance = !assetConfig.GetBoolean("SecondaryInstance", false);

                    MainConsole.Instance.Commands.AddCommand("fs", false,
                            "show assets", "show assets", "Show asset stats",
                            HandleShowAssets);
                    MainConsole.Instance.Commands.AddCommand("fs", false,
                            "show digest", "show digest <ID>", "Show asset digest",
                            HandleShowDigest);
                    MainConsole.Instance.Commands.AddCommand("fs", false,
                            "delete asset", "delete asset <ID>",
                            "Delete asset from database",
                            HandleDeleteAsset);
                    MainConsole.Instance.Commands.AddCommand("fs", false,
                            "import", "import <conn> <table> [<start> <count>]",
                            "Import legacy assets",
                            HandleImportAssets);
                    MainConsole.Instance.Commands.AddCommand("fs", false,
                            "force import", "force import <conn> <table> [<start> <count>]",
                            "Import legacy assets, overwriting current content",
                            HandleImportAssets);
                    MainConsole.Instance.Commands.AddCommand("fs", false,
                        "migrate to s3", "migrate to s3", "Migrate all assets from filesystem to S3",
                        HandleMigrateToS3);
                    MainConsole.Instance.Commands.AddCommand("fs", false,
                        "migrate to fs", "migrate to fs", "Migrate all assets from S3 to filesystem",
                        HandleMigrateToFS);
                }
                else
                {
                    m_isMainInstance = false;
                }
            }

            // Configuração do banco de dados
            string dllName = assetConfig.GetString("StorageProvider", string.Empty);
            string connectionString = assetConfig.GetString("ConnectionString", string.Empty);
            string realm = assetConfig.GetString("Realm", "s3assets");

            int SkipAccessTimeDays = assetConfig.GetInt("DaysBetweenAccessTimeUpdates", 0);

            IConfig dbConfig = config.Configs["DatabaseService"];
            if (dbConfig != null)
            {
                if (dllName.Length == 0)
                    dllName = dbConfig.GetString("StorageProvider", String.Empty);
                if (connectionString.Length == 0)
                    connectionString = dbConfig.GetString("ConnectionString", String.Empty);
            }

            if (string.IsNullOrEmpty(dllName))
                throw new Exception("No StorageProvider configured");
            if (string.IsNullOrEmpty(connectionString))
                throw new Exception("Missing database connection string");

            m_DataConnector = LoadPlugin<IAssetDataPlugin>(dllName);
            if (m_DataConnector == null)
                throw new Exception(string.Format("Could not find a storage interface in the module {0}", dllName));

            m_DataConnector.Initialise(connectionString);//, realm, SkipAccessTimeDays);

            // Configuração do serviço de fallback
            string str = assetConfig.GetString("FallbackService", string.Empty);
            if (str.Length > 0)
            {
                object[] args = new object[] { config };
                m_FallbackService = LoadPlugin<IAssetService>(str, args);
                if (m_FallbackService != null)
                    m_log.Info("[S3ASSETS]: Fallback service loaded");
                else
                    m_log.Error("[S3ASSETS]: Failed to load fallback service");
            }

            // Configura Redis
            string redisConn = assetConfig.GetString("RedisConnection", string.Empty);
            if (!string.IsNullOrEmpty(redisConn))
            {
                redis = ConnectionMultiplexer.Connect(redisConn);
                cacheDb = redis.GetDatabase();
                m_log.Info("[S3ASSETS]: Redis cache enabled");
            }

            // Configuração do S3
            string s3AccessKey = assetConfig.GetString("S3AccessKey", string.Empty);
            string s3SecretKey = assetConfig.GetString("S3SecretKey", string.Empty);
            string s3Region = assetConfig.GetString("S3Region", "us-east-1");
            string S3ServiceURL = assetConfig.GetString("S3ServiceURL", null);

            s3Bucket = assetConfig.GetString("S3Bucket", string.Empty);

            if (string.IsNullOrEmpty(s3AccessKey) || string.IsNullOrEmpty(s3SecretKey) || string.IsNullOrEmpty(s3Bucket) 
                || string.IsNullOrEmpty(S3ServiceURL))
                throw new Exception("Missing S3 configuration");

            // lê o endpoint customizado (ServiceURL) e o flag de path style
            var forcePathStyle = assetConfig.GetBoolean("S3ForcePathStyle", false);

            var s3Config = new AmazonS3Config();

            if (!string.IsNullOrEmpty(S3ServiceURL))
            {
                // usa URL customizada e path-style (requerido por MinIO)
                s3Config.ServiceURL = S3ServiceURL;
                s3Config.ForcePathStyle = forcePathStyle;
            }
            else
            {
                // fallback para AWS S3 “padrão” via região
                var region = assetConfig.GetString("S3Region", "us-east-1");
                s3Config.RegionEndpoint = RegionEndpoint.GetBySystemName(region);
            }

            s3Client = new AmazonS3Client(s3AccessKey, s3SecretKey, s3Config);

            m_log.Info("[S3ASSETS]: FS asset service enabled with S3 storage");
        }

        public virtual bool[] AssetsExist(string[] ids)
        {
            return m_DataConnector.AssetsExist(Array.ConvertAll(ids, UUID.Parse));
        }

        public virtual AssetBase Get(string id)
        {
            return Get(id, out _);
        }

        public AssetBase Get(string id, string ForeignAssetService, bool dummy)
        {
            return null;
        }

        private AssetBase Get(string id, out string sha)
        {
            sha = string.Empty;
            // 1. Tenta Redis
            if (cacheDb != null)
            {
                var cachedData = cacheDb.StringGet(id + ":data");
                var cachedMeta = cacheDb.StringGet(id + ":meta");
                if (cachedData.HasValue && cachedMeta.HasValue)
                {
                    var meta = JsonConvert.DeserializeObject<AssetMetadata>(cachedMeta);
                    sha = meta.Hash;
                    return new AssetBase { Metadata = meta, Data = cachedData };
                }
            }

            // 2. Tenta S3 via DB lookup
            AssetBase metadata;
            string hash = "";
            try
            {
                metadata = m_DataConnector.GetAsset(UUID.Parse(id));
            }
            catch (Exception ex)
            {
                m_log.Warn($"Metadata lookup failed for {id}: {ex.Message}");
                metadata = null;
            }

            if (metadata != null)
            {
                sha = hash;
                byte[] data = null;
                try
                {
                    data = GetS3Data(hash);
                }
                catch (Exception ex)
                {
                    m_log.Warn($"S3 fetch failed for {id}: {ex.Message}");
                    metadata = null;
                }

                if (metadata != null)
                {
                    var asset = new AssetBase { Metadata = metadata.Metadata, Data = data };
                    if (asset.Type == (int)AssetType.Object)
                    {
                        string xml = ExternalRepresentationUtils.SanitizeXml(Utils.BytesToString(data));
                        asset.Data = Utils.StringToBytes(xml);
                    }
                    // cache no Redis
                    if (cacheDb != null)
                    {
                        // metadata.Hash = sha;
                        cacheDb.StringSet(id + ":data", asset.Data, TimeSpan.FromMinutes(30));
                        cacheDb.StringSet(id + ":meta", JsonConvert.SerializeObject(metadata), TimeSpan.FromMinutes(30));
                    }
                    return asset;
                }
            }

            // 3. Fallback DB ou fallback service
            try
            {
                // tenta do DB sem S3
                var assetDb = m_DataConnector.GetAsset(UUID.Parse(id));
                if (assetDb != null)
                    return assetDb;
            }
            catch { }

            if (m_FallbackService != null)
            {
                var assetFb = m_FallbackService.Get(id);
                if (assetFb != null)
                    return assetFb;
            }

            return null;
        }

        public virtual AssetMetadata GetMetadata(string id)
        {
            return m_DataConnector.GetAsset(UUID.Parse(id)).Metadata;
        }

        public virtual byte[] GetData(string id)
        {
            string hash;
            AssetBase dbAsset = m_DataConnector.GetAsset(UUID.Parse(id));
            if(dbAsset== null)
                return null;
            hash = GetSHA256Hash(dbAsset.Data);
            return GetS3Data(hash);
        }

        public bool Get(string id, Object sender, AssetRetrieved handler)
        {
            AssetBase asset = Get(id);
            handler(id, sender, asset);
            return true;
        }

        private byte[] GetS3Data(string hash)
        {
            using var response = s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = s3Bucket,
                Key = hash
            }).Result;
            using var ms = new MemoryStream();
            response.ResponseStream.CopyTo(ms);
            return ms.ToArray();
        }

        public virtual string Store(AssetBase asset)
        {
            string hash = GetSHA256Hash(asset.Data);

            if (!S3ObjectExists(hash))
            {
                using (MemoryStream ms = new MemoryStream(asset.Data))
                {
                    PutObjectRequest request = new PutObjectRequest
                    {
                        BucketName = s3Bucket,
                        Key = hash,
                        InputStream = ms
                    };
                    var result = s3Client.PutObjectAsync(request).Result;
                }
            }

            if (asset.ID.Length == 0)
            {
                if (asset.FullID.IsZero())
                {
                    asset.FullID = UUID.Random();
                }
                asset.ID = asset.FullID.ToString();
            }
            else if (asset.FullID.IsZero())
            {
                UUID uuid = UUID.Zero;
                if (UUID.TryParse(asset.ID, out uuid))
                {
                    asset.FullID = uuid;
                }
                else
                {
                    asset.FullID = UUID.Random();
                }
            }

            if (!m_DataConnector.StoreAsset(asset))
            {
                if (asset.Metadata.Type == -2)
                    return asset.ID;

                return UUID.Zero.ToString();
            }
            else
            {
                return asset.ID;
            }
        }

        private bool S3ObjectExists(string hash)
        {
            try { s3Client.GetObjectMetadataAsync(s3Bucket, hash).Wait(); return true; }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound) { return false; }
        }

        public bool UpdateContent(string id, byte[] data)
        {
            return false;
        }

        public virtual bool Delete(string id)
        {
            m_DataConnector.Delete(id);
            return true;
        }

        private void HandleShowAssets(string module, string[] args)
        {
            int num = 0;// m_DataConnector.;
            MainConsole.Instance.Output(string.Format("Total asset count: {0}", num));
        }

        private void HandleShowDigest(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: show digest <ID>");
                return;
            }

            string hash;
            AssetBase asset = Get(args[2], out hash);

            if (asset == null || asset.Data.Length == 0)
            {
                MainConsole.Instance.Output("Asset not found");
                return;
            }

            MainConsole.Instance.Output(String.Format("Name: {0}", asset.Name));
            MainConsole.Instance.Output(String.Format("Description: {0}", asset.Description));
            MainConsole.Instance.Output(String.Format("Type: {0}", asset.Type));
            MainConsole.Instance.Output(String.Format("Content-type: {0}", asset.Metadata.ContentType));
            MainConsole.Instance.Output(String.Format("Flags: {0}", asset.Metadata.Flags.ToString()));
            MainConsole.Instance.Output(String.Format("S3 key: {0}", hash));

            for (int i = 0; i < 5; i++)
            {
                int off = i * 16;
                if (asset.Data.Length <= off)
                    break;
                int len = 16;
                if (asset.Data.Length < off + len)
                    len = asset.Data.Length - off;

                byte[] line = new byte[len];
                Array.Copy(asset.Data, off, line, 0, len);

                string text = BitConverter.ToString(line);
                MainConsole.Instance.Output(String.Format("{0:x4}: {1}", off, text));
            }
        }

        private void HandleDeleteAsset(string module, string[] args)
        {
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: delete asset <ID>");
                return;
            }

            AssetBase asset = Get(args[2]);

            if (asset == null || asset.Data.Length == 0)
            {
                MainConsole.Instance.Output("Asset not found");
                return;
            }

            m_DataConnector.Delete(args[2]);

            MainConsole.Instance.Output("Asset deleted");
        }

        private void HandleImportAssets(string module, string[] args)
        {
            MainConsole.Instance.Output("Not implemented yet: import <conn> <table> [<start> <count>]");
            /*
            bool force = false;
            if (args[0] == "force")
            {
                force = true;
                List<string> list = new List<string>(args);
                list.RemoveAt(0);
                args = list.ToArray();
            }
            if (args.Length < 3)
            {
                MainConsole.Instance.Output("Syntax: import <conn> <table> [<start> <count>]");
            }
            else
            {
                string conn = args[1];
                string table = args[2];
                int start = 0;
                int count = -1;
                if (args.Length > 3)
                {
                    start = Convert.ToInt32(args[3]);
                }
                if (args.Length > 4)
                {
                    count = Convert.ToInt32(args[4]);
                }
                m_DataConnector.Import(conn, table, start, count, force, new FSStoreDelegate(Store, true));
            }
            */
        }

        public AssetBase GetCached(string id)
        {
            return Get(id);
        }

        public void Get(string id, string ForeignAssetService, bool StoreOnLocalGrid, SimpleAssetRetrieved callBack)
        {
            return;
        }

        private string GetSHA256Hash(byte[] data)
        {
            byte[] hash;
            using (SHA256 sha = SHA256.Create())
                hash = sha.ComputeHash(data);

            return BitConverter.ToString(hash).Replace("-", String.Empty);
        }
        
        
        private void HandleMigrateToS3(string module, string[] args)
        {
            string fsBase =  assetConfig.GetString("BaseDirectory", string.Empty);
            if (string.IsNullOrEmpty(fsBase))
            {
                MainConsole.Instance.Output("BaseDirectory not specified in configuration.");
                return;
            }

            int migratedCount = 0;
            int failedCount = 0;

            // Obter todos os metadados dos assets
            List<AssetMetadata> allAssets = m_DataConnector.GetAllAssets();

            foreach (var metadata in allAssets)
            {
                if(metadata.Hash == null)
                {
                    // string hash = GetSHA256Hash(metadata.);
                }
                string hash = metadata.Hash; // O hash é o identificador do arquivo
                string filePath = Path.Combine(fsBase, HashToFile(hash));

                if (File.Exists(filePath))
                {
                    try
                    {
                        byte[] data = File.ReadAllBytes(filePath);
                        using (MemoryStream ms = new MemoryStream(data))
                        {
                            PutObjectRequest request = new PutObjectRequest
                            {
                                BucketName = s3Bucket,
                                Key = hash,
                                InputStream = ms
                            };
                            var resultPut = s3Client.PutObjectAsync(request).Result;
                        }
                        migratedCount++;
                    }
                    catch (Exception ex)
                    {
                        m_log.Error($"Failed to migrate asset {metadata.ID} to S3: {ex.Message}");
                        failedCount++;
                    }
                }
                else
                {
                    m_log.Warn($"Asset file not found in FS: {filePath}");
                }
            }

            MainConsole.Instance.Output($"Migration to S3 completed. Migrated: {migratedCount}, Failed: {failedCount}");
        }
        private void HandleMigrateToFS(string module, string[] args)
        {
            string fsBase = assetConfig.GetString("BaseDirectory", string.Empty);
            if (string.IsNullOrEmpty(fsBase))
            {
                MainConsole.Instance.Output("BaseDirectory not specified in configuration.");
                return;
            }

            int migratedCount = 0;
            int failedCount = 0;

            // Obter todos os metadados dos assets
            List<AssetMetadata> allAssets = m_DataConnector.GetAllAssets();

            foreach (var metadata in allAssets)
            {
                string hash = metadata.Hash; // O hash é a chave no S3
                string filePath = Path.Combine(fsBase, HashToFile(hash));

                try
                {
                    GetObjectRequest request = new GetObjectRequest
                    {
                        BucketName = s3Bucket,
                        Key = hash
                    };
                    using (GetObjectResponse response = s3Client.GetObjectAsync(request).Result)
                    using (Stream responseStream = response.ResponseStream)
                    using (FileStream fs = new FileStream(filePath, FileMode.Create))
                    {
                        responseStream.CopyTo(fs);
                    }
                    migratedCount++;
                }
                catch (AmazonS3Exception ex)
                {
                    if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        m_log.Warn($"Asset not found in S3: {hash}");
                    }
                    else
                    {
                        m_log.Error($"Failed to migrate asset {metadata.ID} to FS: {ex.Message}");
                        failedCount++;
                    }
                }
                catch (Exception ex)
                {
                    m_log.Error($"Failed to migrate asset {metadata.ID} to FS: {ex.Message}");
                    failedCount++;
                }
            }

            MainConsole.Instance.Output($"Migration to FS completed. Migrated: {migratedCount}, Failed: {failedCount}");
        }
        private string HashToFile(string hash)
        {
            return Path.Combine(hash.Substring(0, 2), Path.Combine(hash.Substring(2, 2), Path.Combine(hash.Substring(4, 2), hash.Substring(6))));
        }
    }
}