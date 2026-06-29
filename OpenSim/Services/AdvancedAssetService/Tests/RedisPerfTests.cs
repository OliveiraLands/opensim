using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using OpenMetaverse;
using OpenSim.Services.AdvancedAssetService;

namespace OpenSim.Services.AdvancedAssetService.Tests
{
    public static class RedisPerfTests
    {
        public static void RunBenchmark()
        {
            Console.WriteLine("\n=================================================================");
            Console.WriteLine("RUNNING CAPS TEXTURE SERVER REDIS/GARNET PERFORMANCE BENCHMARK");
            Console.WriteLine("=================================================================");

            // 1. Locate index.db
            string baseDir = AppDomain.CurrentDomain.BaseDirectory;
            string storagePath = Path.Combine(baseDir, "asset_packs");
            if (!Directory.Exists(storagePath))
            {
                storagePath = Path.Combine(baseDir, "..", "asset_packs");
            }
            string indexDbPath = Path.Combine(storagePath, "index.db");

            Console.WriteLine($"Looking for SQLite database at: {indexDbPath}");
            var uuids = new List<string>();

            if (File.Exists(indexDbPath))
            {
                try
                {
                    var sqliteConnType = Type.GetType("System.Data.SQLite.SQLiteConnection, System.Data.SQLite");
                    if (sqliteConnType != null)
                    {
                        using (dynamic conn = Activator.CreateInstance(sqliteConnType, $"Data Source={indexDbPath};Version=3;Read Only=True;"))
                        {
                            conn.Open();
                            using (dynamic cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = "SELECT uuid FROM asset_map WHERE type = 0 LIMIT 10";
                                using (dynamic reader = cmd.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        uuids.Add(reader.GetString(0));
                                    }
                                }
                            }
                        }
                        Console.WriteLine($"Found {uuids.Count} texture assets in the SQLite database.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not read index.db: {ex.Message}");
                }
            }

            // Fallback: If no texture assets found, look for any type
            if (uuids.Count == 0 && File.Exists(indexDbPath))
            {
                try
                {
                    var sqliteConnType = Type.GetType("System.Data.SQLite.SQLiteConnection, System.Data.SQLite");
                    if (sqliteConnType != null)
                    {
                        using (dynamic conn = Activator.CreateInstance(sqliteConnType, $"Data Source={indexDbPath};Version=3;Read Only=True;"))
                        {
                            conn.Open();
                            using (dynamic cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = "SELECT uuid FROM asset_map LIMIT 10";
                                using (dynamic reader = cmd.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        uuids.Add(reader.GetString(0));
                                    }
                                }
                            }
                        }
                    }
                    if (uuids.Count > 0)
                        Console.WriteLine($"Found {uuids.Count} non-texture assets to use for caching test.");
                }
                catch {}
            }

            // Fallback 2: If still no assets, write a test one
            if (uuids.Count == 0)
            {
                Console.WriteLine("No assets found in index.db. Generating mock asset...");
                string mockUuid = "00000000-0000-0000-0000-000000000999";
                byte[] mockPayload = new byte[1024 * 100]; // 100 KB mock texture
                new Random().NextBytes(mockPayload);

                using (var manager = new PackFileManager(storagePath))
                {
                    manager.StoreAssetData(mockUuid, mockPayload, 0, "PerfMockTexture");
                    
                    // Wait up to 5 seconds for the background write thread to process and index the asset
                    int retries = 50;
                    while (retries > 0)
                    {
                        sbyte type;
                        string name;
                        byte[] cached = manager.GetAssetData(mockUuid, out type, out name, false);
                        if (cached != null && cached.Length > 0)
                        {
                            // Allow batch timer to flush and commit transaction
                            System.Threading.Thread.Sleep(2000);
                            break;
                        }
                        System.Threading.Thread.Sleep(100);
                        retries--;
                    }
                }
                uuids.Add(mockUuid);
            }

            // 2. Perform HTTP queries to local server (AdvCapsServer) at port 8004
            string targetUrl = "http://127.0.0.1:8004/CAPS/GetTexture/";
            Console.WriteLine($"Target Server URL: {targetUrl}");

            var handler = new HttpClientHandler { UseProxy = false };
            using (var client = new HttpClient(handler))
            {
                // Ping status first
                try
                {
                    var statusRes = client.GetAsync("http://127.0.0.1:8004/status").Result;
                    Console.WriteLine($"Server status check: {statusRes.StatusCode} ({statusRes.Content.ReadAsStringAsync().Result})");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Error: Cannot reach CAPS server at port 8004. Make sure AdvCapsServer is running! Error: {ex.Message}");
                    Console.ResetColor();
                    return;
                }

                double totalMissMs = 0;
                double totalHitMs = 0;
                int missCount = 0;
                int hitCount = 0;

                Console.WriteLine("\n-----------------------------------------------------------------");
                Console.WriteLine("{0,-40} | {1,-12} | {2,-12}", "Texture UUID", "1st (Miss)", "2nd (Hit)");
                Console.WriteLine("-----------------------------------------------------------------");

                foreach (var uuid in uuids)
                {
                    // Request 1: Cache Miss (reads from disk, writes to Redis)
                    var sw = Stopwatch.StartNew();
                    var res1 = client.GetAsync(targetUrl + uuid).Result;
                    sw.Stop();
                    double missMs = sw.Elapsed.TotalMilliseconds;

                    if (res1.StatusCode != HttpStatusCode.OK)
                    {
                        Console.WriteLine($"{uuid} | FAILED (Status: {res1.StatusCode})");
                        continue;
                    }

                    // Request 2: Cache Hit (reads from Garnet RAM)
                    sw.Restart();
                    var res2 = client.GetAsync(targetUrl + uuid).Result;
                    sw.Stop();
                    double hitMs = sw.Elapsed.TotalMilliseconds;

                    if (res2.StatusCode != HttpStatusCode.OK)
                    {
                        Console.WriteLine($"{uuid} | FAILED on 2nd query");
                        continue;
                    }

                    Console.WriteLine("{0,-40} | {1,8:F2} ms | {2,8:F2} ms", uuid, missMs, hitMs);

                    totalMissMs += missMs;
                    totalHitMs += hitMs;
                    missCount++;
                    hitCount++;
                }

                Console.WriteLine("-----------------------------------------------------------------");
                if (missCount > 0)
                {
                    double avgMiss = totalMissMs / missCount;
                    double avgHit = totalHitMs / hitCount;
                    double speedup = avgMiss / avgHit;

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Average Cache Miss (Disk Read):  {avgMiss:F2} ms");
                    Console.WriteLine($"Average Cache Hit (Garnet RAM): {avgHit:F2} ms");
                    Console.WriteLine($"Performance Speedup Factor:     {speedup:F1}x faster");
                    Console.ResetColor();
                }
                Console.WriteLine("=================================================================\n");
            }
        }
    }
}
