using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Services.AdvancedAssetService;
using StackExchange.Redis;

namespace OpenSim.Services.AdvancedAssetService.AdvCapsServer
{
    public class Program
    {
        public static readonly ManualResetEvent ExitEvent = new ManualResetEvent(false);
        private static PackFileManager m_PackManager;
        private static bool m_Verbose = false;

        // Redis cache fields
        private static ConnectionMultiplexer m_RedisConn;
        private static IDatabase m_RedisDb;
        private static bool m_UseRedis = false;
        private static int m_RedisTTL = 86400; // 24 hours in seconds default

        static void Main(string[] args)
        {
            ServerMain(args);
        }

        public static void ServerMain(string[] args)
        {
            Console.WriteLine("=================================================================");
            Console.WriteLine("Advanced Asset Service - Parallel CAPS GetTexture Server (AdvCapsServer)");
            Console.WriteLine("=================================================================");

            // 1. Config Defaults
            int port = 8004;
            string host = "0.0.0.0";
            string storagePath = "asset_packs";
            bool verifyOnRead = false;
            string redisConnectionString = "localhost:6379,abortConnect=false";

            // 2. Load Robust.ini Configuration
            string configPath = "Robust.ini";
            if (args.Length > 0 && File.Exists(args[0]))
            {
                configPath = args[0];
            }
            else if (!File.Exists(configPath) && File.Exists("../Robust.ini"))
            {
                configPath = "../Robust.ini";
            }
            else if (!File.Exists(configPath) && File.Exists("../../Robust.ini"))
            {
                configPath = "../../Robust.ini";
            }
            else if (!File.Exists(configPath) && File.Exists("../../../Robust.ini"))
            {
                configPath = "../../../Robust.ini";
            }

            Console.WriteLine($"Reading configuration from: {Path.GetFullPath(configPath)}");

            if (File.Exists(configPath))
            {
                try
                {
                    var configSource = new IniConfigSource(configPath);
                    var assetSec = configSource.Configs["AssetService"];
                    var capsSec = configSource.Configs["CAPSTextureServer"];

                    if (assetSec != null)
                    {
                        storagePath = assetSec.GetString("StoragePath", storagePath);
                        verifyOnRead = assetSec.GetBoolean("VerifyOnRead", verifyOnRead);
                    }

                    if (capsSec != null)
                    {
                        port = capsSec.GetInt("Port", port);
                        host = capsSec.GetString("Host", host);
                        storagePath = capsSec.GetString("StoragePath", storagePath);
                        verifyOnRead = capsSec.GetBoolean("VerifyOnRead", verifyOnRead);
                        m_Verbose = capsSec.GetBoolean("Verbose", m_Verbose);

                        // Redis Settings
                        m_UseRedis = capsSec.GetBoolean("UseRedis", m_UseRedis);
                        redisConnectionString = capsSec.GetString("RedisConnectionString", redisConnectionString);
                        m_RedisTTL = capsSec.GetInt("RedisTTL", m_RedisTTL);
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"Warning: Failed to parse config file: {ex.Message}");
                    Console.ResetColor();
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Robust.ini not found. Using defaults.");
                Console.ResetColor();
            }

            // Standardize paths
            if (!Path.IsPathRooted(storagePath))
            {
                storagePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, storagePath);
            }

            Console.WriteLine($"Storage Path: {storagePath}");
            Console.WriteLine($"Verify On Read: {verifyOnRead}");
            Console.WriteLine($"Verbose logs: {m_Verbose}");
            Console.WriteLine($"Redis Cache Enabled: {m_UseRedis}");
            if (m_UseRedis)
            {
                Console.WriteLine($"Redis Connection String: {redisConnectionString}");
                Console.WriteLine($"Redis Default TTL: {m_RedisTTL} seconds");
            }
            Console.WriteLine($"Binding HttpListener to: http://{host}:{port}/");

            // 3. Initialize Shared PackFileManager (Read-Only context)
            if (!Directory.Exists(storagePath))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error: Storage directory does not exist: {storagePath}");
                Console.ResetColor();
                return;
            }

            try
            {
                m_PackManager = new PackFileManager(storagePath);
                Console.WriteLine("Shared PackFileManager initialized successfully.");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Fatal Error: Failed to initialize PackFileManager: {ex.Message}");
                Console.ResetColor();
                return;
            }

            // 4. Initialize Redis Cache (Microsoft Garnet / Redis)
            if (m_UseRedis)
            {
                try
                {
                    Console.WriteLine("Parsing Redis configuration and setting timeouts...");
                    var options = ConfigurationOptions.Parse(redisConnectionString);
                    options.ConnectTimeout = 1000; // 1 second connect timeout
                    options.SyncTimeout = 250;     // 250 ms sync command timeout
                    options.AsyncTimeout = 250;    // 250 ms async command timeout

                    Console.WriteLine("Establishing Redis/Garnet Connection...");
                    m_RedisConn = ConnectionMultiplexer.Connect(options);
                    m_RedisDb = m_RedisConn.GetDatabase();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Connected to Redis/Garnet successfully.");
                    Console.ResetColor();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Warning: Failed to connect to Redis/Garnet Cache: {ex.Message}");
                    Console.WriteLine("Falling back to raw Disk/PackFile reads (Redis caching disabled).");
                    Console.ResetColor();
                    m_UseRedis = false;
                }
            }

            // 5. Initialize HttpListener
            HttpListener listener = new HttpListener();
            try
            {
                if (host == "0.0.0.0" || host == "*")
                {
                    listener.Prefixes.Add($"http://*:{port}/");
                }
                else
                {
                    listener.Prefixes.Add($"http://{host}:{port}/");
                }
                
                listener.Start();
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"HTTP Server listening successfully on port {port}.");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Fatal Error: Failed to start HTTP Listener: {ex.Message}");
                Console.WriteLine("Make sure you run as administrator if binding to all interfaces (* or 0.0.0.0), or try a higher port.");
                Console.ResetColor();
                
                // Clean up PackManager before exiting
                try { m_PackManager?.Dispose(); } catch {}
                try { m_RedisConn?.Close(); } catch {}
                return;
            }

            // 6. Asynchronous Request Processing Loop
            var listenTask = Task.Run(async () =>
            {
                while (listener.IsListening)
                {
                    try
                    {
                        HttpListenerContext context = await listener.GetContextAsync();
                        _ = Task.Run(() => ProcessRequestAsync(context, verifyOnRead));
                    }
                    catch (HttpListenerException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Listener dispatch error: {ex.Message}");
                    }
                }
            });

            Console.WriteLine("=================================================================");
            Console.WriteLine("CAPS Texture Server is running. Press CTRL+C to terminate.");
            Console.WriteLine("=================================================================");

            // Wait for exit
            ExitEvent.Reset();
            Console.CancelKeyPress += (sender, eventArgs) => {
                eventArgs.Cancel = true;
                ExitEvent.Set();
            };
            
            ExitEvent.WaitOne();

            Console.WriteLine("Stopping HTTP Server...");
            try
            {
                listener.Stop();
                listener.Close();
            }
            catch {}

            try
            {
                m_PackManager?.Dispose();
            }
            catch {}

            try
            {
                m_RedisConn?.Close();
                m_RedisConn?.Dispose();
            }
            catch {}

            Console.WriteLine("CAPS Texture Server stopped.");
        }

        private static async Task ProcessRequestAsync(HttpListenerContext context, bool verifyOnRead)
        {
            var request = context.Request;
            var response = context.Response;

            try
            {
                string path = request.Url.AbsolutePath;
                string textureId = null;

                if (path.StartsWith("/CAPS/GetTexture", StringComparison.OrdinalIgnoreCase))
                {
                    string[] parts = path.TrimEnd('/').Split('/');
                    if (parts.Length > 3)
                    {
                        textureId = parts[3];
                    }
                    else
                    {
                        textureId = request.QueryString["texture_id"];
                    }
                }
                else if (path.Equals("/status", StringComparison.OrdinalIgnoreCase))
                {
                    response.StatusCode = (int)HttpStatusCode.OK;
                    response.ContentType = "application/json";
                    using (var writer = new StreamWriter(response.OutputStream))
                    {
                        await writer.WriteAsync($"{{\"Status\":\"Online\",\"Service\":\"AAS CAPS Texture Server\",\"StoragePath\":\"{m_PackManager.ToString()}\",\"RedisCacheEnabled\":{m_UseRedis}}}");
                    }
                    response.Close();
                    return;
                }

                if (string.IsNullOrEmpty(textureId) || !UUID.TryParse(textureId, out UUID uuid))
                {
                    response.StatusCode = (int)HttpStatusCode.BadRequest;
                    using (var writer = new StreamWriter(response.OutputStream))
                    {
                        await writer.WriteAsync("Invalid or missing texture UUID.");
                    }
                    response.Close();
                    return;
                }

                byte[] data = null;
                string redisKey = $"texture:{uuid}";

                // 1. Try serving from Redis cache (Garnet)
                if (m_UseRedis && m_RedisDb != null)
                {
                    try
                    {
                        data = await m_RedisDb.StringGetAsync(redisKey);
                        if (data != null && data.Length > 0)
                        {
                            ServeResponse(response, data, uuid, "CachedTexture", 0, true);
                            return;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (m_Verbose)
                        {
                            Console.WriteLine($"[CAPS TEXTURE] Redis read exception for {uuid}: {ex.Message}");
                        }
                    }
                }

                // 2. Cache Miss - Read directly from the PackFileManager (shared concurrent file reading)
                sbyte type;
                string name;
                data = m_PackManager.GetAssetData(uuid.ToString(), out type, out name, verifyOnRead);

                if (data == null || data.Length == 0)
                {
                    if (m_Verbose)
                    {
                        Console.WriteLine($"[CAPS TEXTURE] Texture {uuid} not found in pack files.");
                    }
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                    response.Close();
                    return;
                }

                // 3. Asynchronously populate Redis cache for subsequent hits (Fire and Forget)
                if (m_UseRedis && m_RedisDb != null)
                {
                    try
                    {
                        _ = m_RedisDb.StringSetAsync(redisKey, data, TimeSpan.FromSeconds(m_RedisTTL));
                    }
                    catch (Exception ex)
                    {
                        if (m_Verbose)
                        {
                            Console.WriteLine($"[CAPS TEXTURE] Redis write exception for {uuid}: {ex.Message}");
                        }
                    }
                }

                // 4. Serve the response from the PackFile payload
                ServeResponse(response, data, uuid, name, type, false);
            }
            catch (Exception ex)
            {
                if (m_Verbose)
                {
                    Console.WriteLine($"Error processing request: {ex.Message}");
                }
                try
                {
                    response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    response.Close();
                }
                catch {}
            }
        }

        private static void ServeResponse(HttpListenerResponse response, byte[] data, UUID uuid, string name, sbyte type, bool cacheHit)
        {
            if (m_Verbose)
            {
                string source = cacheHit ? "Redis/Garnet Cache" : "Disk PackFile";
                Console.WriteLine($"[CAPS TEXTURE] Serving {uuid} ({name}) from {source}, Type: {type}, Size: {data.Length} bytes");
            }

            response.ContentType = "image/x-j2c";
            // Texture assets are immutable, instruct viewer to cache aggressively
            response.Headers.Add("Cache-Control", "public, max-age=31536000, immutable");
            response.ContentLength64 = data.Length;

            response.OutputStream.Write(data, 0, data.Length);
            response.Close();
        }
    }
}
