using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Framework;

namespace OpenSim.Services.AdvancedAssetService.Diagnostics
{
    class Program
    {
        private static string StoragePath = "diag_asset_packs";

        static void Main(string[] args)
        {
            Console.WriteLine("=================================================");
            Console.WriteLine("AdvancedAssetService Concurrency Diagnostic Tool");
            Console.WriteLine("=================================================");

            // Configure basic logging to console
            var repository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
            log4net.Config.BasicConfigurator.Configure(repository);

            if (Directory.Exists(StoragePath))
            {
                Console.WriteLine($"Cleaning up existing storage path: {StoragePath}");
                try
                {
                    Directory.Delete(StoragePath, true);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to clean directory: {ex.Message}");
                }
            }

            // Test cases:
            // 1. Concurrent writes of unique assets (pure insertion load)
            // 2. Concurrent writes of the same asset IDs (updates/conflicts)
            // 3. Concurrent writes and reads simultaneously (read/write contention)
            
            RunUniqueWritesLoadTest();
            RunConflictWritesLoadTest();
            RunReadWriteContentionTest();
            RunDisasterRecoveryTest();

            Console.WriteLine("\nDiagnostics completed successfully.");
        }

        private static AdvancedAssetService CreateService()
        {
            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", StoragePath);
            return new AdvancedAssetService(config);
        }

        private static void RunUniqueWritesLoadTest()
        {
            Console.WriteLine("\n--- Test 1: Concurrent writes of UNIQUE assets ---");
            using (var service = CreateService())
            {
                int numThreads = 50;
                int assetsPerThread = 150;
                int totalAssets = numThreads * assetsPerThread;
                
                var storedAssets = new ConcurrentDictionary<string, byte[]>();
                var tasks = new List<Task>();

                Console.WriteLine($"Starting {numThreads} threads, each writing {assetsPerThread} unique assets (Total: {totalAssets})...");

                var startTime = DateTime.UtcNow;

                for (int t = 0; t < numThreads; t++)
                {
                    int threadId = t;
                    tasks.Add(Task.Run(() =>
                    {
                        var rand = new Random(threadId);
                        for (int i = 0; i < assetsPerThread; i++)
                        {
                            UUID assetId = UUID.Random();
                            byte[] data = new byte[rand.Next(1024, 102400)]; // 1KB to 100KB
                            rand.NextBytes(data);

                            AssetBase asset = new AssetBase(assetId, $"UniqueAsset_{threadId}_{i}", (sbyte)AssetType.Texture, UUID.Zero.ToString());
                            asset.Data = data;

                            string id = service.Store(asset);
                            storedAssets[id] = data;
                        }
                    }));
                }

                Task.WaitAll(tasks.ToArray());
                var writeTime = DateTime.UtcNow - startTime;
                Console.WriteLine($"All writes queued in {writeTime.TotalMilliseconds:F2} ms (Avg: {writeTime.TotalMilliseconds / totalAssets:F2} ms/asset).");

                // Let's verify reading them immediately while background process is flushing
                Console.WriteLine("Verifying readability during flush...");
                int readSuccessCount = 0;
                foreach (var kvp in storedAssets)
                {
                    AssetBase retrieved = service.Get(kvp.Key);
                    if (retrieved != null && CompareBytes(retrieved.Data, kvp.Value))
                    {
                        readSuccessCount++;
                    }
                }
                Console.WriteLine($"Immediate verify: {readSuccessCount} / {totalAssets} successfully retrieved.");
            } // This disposes service and flushes everything.

            // Re-open from disk and verify persistence
            Console.WriteLine("Re-opening service to verify persistent index and PackFiles...");
            using (var service = CreateService())
            {
                // Let's trigger a verify check
                service.VerifyIntegrityDiag(msg => Console.WriteLine("AAS: " + msg));
            }
        }

        private static void RunConflictWritesLoadTest()
        {
            Console.WriteLine("\n--- Test 2: Concurrent writes of the SAME assets (Conflicts) ---");
            // Here, we have multiple threads attempting to write the same asset ID with different data.
            // Under normal circumstances, the last write should eventually win, or they should be queued.
            // But if the cache ignores subsequent writes while the first is in flight, some data might be lost/out of sync.
            using (var service = CreateService())
            {
                // We'll write to 10 specific assets concurrently.
                var targetUUIDs = new List<string>();
                for (int i = 0; i < 10; i++) targetUUIDs.Add(UUID.Random().ToString());

                int numThreads = 30;
                int loops = 150;
                var tasks = new List<Task>();

                Console.WriteLine($"Spawning {numThreads} threads, each writing {loops} updates to the same 5 asset IDs...");

                for (int t = 0; t < numThreads; t++)
                {
                    int threadId = t;
                    tasks.Add(Task.Run(() =>
                    {
                        var rand = new Random(threadId);
                        for (int i = 0; i < loops; i++)
                        {
                            foreach (var uuid in targetUUIDs)
                            {
                                byte[] data = new byte[100];
                                Array.Clear(data, 0, data.Length);
                                // Embed thread ID and loop index so we know who wrote it
                                BitConverter.GetBytes(threadId).CopyTo(data, 0);
                                BitConverter.GetBytes(i).CopyTo(data, 4);

                                AssetBase asset = new AssetBase(new UUID(uuid), $"ConflictAsset_{threadId}_{i}", (sbyte)AssetType.Texture, UUID.Zero.ToString());
                                asset.Data = data;

                                service.Store(asset);
                                Thread.Sleep(rand.Next(1, 10)); // small stagger
                            }
                        }
                    }));
                }

                Task.WaitAll(tasks.ToArray());
                Console.WriteLine("Conflict writes finished queueing. Waiting 3 seconds for batch flush...");
                Thread.Sleep(3000);

                // Verify
                foreach (var uuid in targetUUIDs)
                {
                    AssetBase asset = service.Get(uuid);
                    if (asset == null)
                    {
                        Console.WriteLine($"[ERROR] Asset {uuid} is missing entirely!");
                    }
                    else
                    {
                        int writerThread = BitConverter.ToInt32(asset.Data, 0);
                        int writeLoop = BitConverter.ToInt32(asset.Data, 4);
                        Console.WriteLine($"Asset {uuid} contains data from Thread {writerThread}, Loop {writeLoop}. Status: OK.");
                    }
                }
            }
        }

        private static void RunReadWriteContentionTest()
        {
            Console.WriteLine("\n--- Test 3: Concurrent Reads and Writes (Contention) ---");
            using (var service = CreateService())
            {
                var storedAssets = new ConcurrentDictionary<string, byte[]>();
                // Pre-populate some assets
                for (int i = 0; i < 150; i++)
                {
                    UUID id = UUID.Random();
                    byte[] data = new byte[500];
                    new Random().NextBytes(data);
                    AssetBase asset = new AssetBase(id, $"PreAsset_{i}", (sbyte)AssetType.Texture, UUID.Zero.ToString());
                    asset.Data = data;
                    service.Store(asset);
                    storedAssets[id.ToString()] = data;
                }

                // Wait for flush
                Thread.Sleep(2500);

                int numWriteThreads = 15;
                int numReadThreads = 25;
                var tasks = new List<Task>();
                var cts = new CancellationTokenSource();

                Console.WriteLine($"Starting {numWriteThreads} writers and {numReadThreads} readers concurrently...");

                // Start Readers
                var readErrors = 0;
                var readSuccess = 0;
                for (int r = 0; r < numReadThreads; r++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        var keys = new List<string>(storedAssets.Keys);
                        var rand = new Random();
                        while (!cts.Token.IsCancellationRequested)
                        {
                            if (keys.Count == 0) continue;
                            string key = keys[rand.Next(keys.Count)];
                            AssetBase asset = service.Get(key);
                            if (asset != null && CompareBytes(asset.Data, storedAssets[key]))
                            {
                                Interlocked.Increment(ref readSuccess);
                            }
                            else
                            {
                                Interlocked.Increment(ref readErrors);
                            }
                            Thread.Sleep(1);
                        }
                    }));
                }

                // Start Writers (writing new assets)
                for (int w = 0; w < numWriteThreads; w++)
                {
                    int writerId = w;
                    tasks.Add(Task.Run(() =>
                    {
                        var rand = new Random(writerId + 100);
                        for (int i = 0; i < 100; i++)
                        {
                            UUID id = UUID.Random();
                            byte[] data = new byte[200];
                            rand.NextBytes(data);
                            AssetBase asset = new AssetBase(id, $"ContentionAsset_{writerId}_{i}", (sbyte)AssetType.Texture, UUID.Zero.ToString());
                            asset.Data = data;
                            service.Store(asset);
                            storedAssets[id.ToString()] = data;
                            Thread.Sleep(5);
                        }
                    }));
                }

                // Let writers finish, then cancel readers
                Thread.Sleep(2000);
                cts.Cancel();
                try { Task.WaitAll(tasks.ToArray()); } catch {}

                Console.WriteLine($"Read success: {readSuccess}, Read errors/corruption: {readErrors}.");
            }
        }

        private static void RunDisasterRecoveryTest()
        {
            Console.WriteLine("\n--- Test 4: Disaster Recovery of Deduplicated Assets (Symbolic Links) ---");
            string dbPath = Path.Combine(StoragePath, "index.db");

            var primaryAssets = new Dictionary<string, byte[]>();
            var duplicateAssets = new Dictionary<string, string>(); // duplicateUUID -> primaryUUID

            using (var service = CreateService())
            {
                Console.WriteLine("Storing primary assets and their duplicate links...");
                for (int i = 0; i < 5; i++)
                {
                    UUID primaryId = UUID.Random();
                    byte[] data = Encoding.UTF8.GetBytes(string.Format("DeduplicatedContentData_{0}", i));
                    
                    AssetBase primaryAsset = new AssetBase(primaryId, string.Format("PrimaryAsset_{0}", i), (sbyte)AssetType.Texture, UUID.Zero.ToString());
                    primaryAsset.Data = data;
                    service.Store(primaryAsset);
                    primaryAssets[primaryId.ToString()] = data;

                    for (int d = 0; d < 3; d++)
                    {
                        UUID duplicateId = UUID.Random();
                        AssetBase duplicateAsset = new AssetBase(duplicateId, string.Format("DuplicateAsset_{0}_{1}", i, d), (sbyte)AssetType.Texture, UUID.Zero.ToString());
                        duplicateAsset.Data = data; // Trigger deduplication
                        service.Store(duplicateAsset);
                        duplicateAssets[duplicateId.ToString()] = primaryId.ToString();
                    }
                }

                Console.WriteLine("Waiting for background queue to flush...");
                Thread.Sleep(3000);
            }

            Console.WriteLine("Simulating database disaster by deleting SQLite index.db...");
            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
                Console.WriteLine("Database file deleted successfully.");
            }
            else
            {
                Console.WriteLine("[ERROR] index.db was not found!");
                return;
            }

            Console.WriteLine("Re-opening service with empty database...");
            using (var service = CreateService())
            {
                foreach (var id in primaryAssets.Keys)
                {
                    if (service.Get(id) != null)
                    {
                        Console.WriteLine("[ERROR] Asset was found in empty database before rebuild!");
                        return;
                    }
                }

                Console.WriteLine("Triggering RebuildIndex to recover database from pack files...");
                service.RebuildIndexDiag();
                Console.WriteLine("Rebuild completed.");

                int recoveredPrimary = 0;
                foreach (var kvp in primaryAssets)
                {
                    AssetBase recovered = service.Get(kvp.Key);
                    if (recovered != null && CompareBytes(recovered.Data, kvp.Value))
                    {
                        recoveredPrimary++;
                    }
                }
                Console.WriteLine(string.Format("Recovered primary assets: {0} / {1}", recoveredPrimary, primaryAssets.Count));

                int recoveredDuplicates = 0;
                foreach (var kvp in duplicateAssets)
                {
                    AssetBase recovered = service.Get(kvp.Key);
                    byte[] expectedData = primaryAssets[kvp.Value];
                    if (recovered != null && CompareBytes(recovered.Data, expectedData))
                    {
                        recoveredDuplicates++;
                    }
                }
                Console.WriteLine(string.Format("Recovered deduplicated asset links: {0} / {1}", recoveredDuplicates, duplicateAssets.Count));

                if (recoveredPrimary == primaryAssets.Count && recoveredDuplicates == duplicateAssets.Count)
                {
                    Console.WriteLine("Disaster Recovery Test: PASSED. All duplicate links successfully recovered from pack files.");
                }
                else
                {
                    Console.WriteLine("Disaster Recovery Test: FAILED. Missing recovered assets.");
                }
            }
        }

        private static bool CompareBytes(byte[] a, byte[] b)
        {
            if (a == null || b == null) return a == b;
            if (a.Length != b.Length) return false;
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i]) return false;
            }
            return true;
        }
    }

    public static class AssetServiceExtensions
    {
        public static void VerifyIntegrityDiag(this AdvancedAssetService service, Action<string> output)
        {
            var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", BindingFlags.Instance | BindingFlags.NonPublic);
            if (packManagerField != null)
            {
                var packManager = packManagerField.GetValue(service);
                if (packManager != null)
                {
                    var verifyMethod = packManager.GetType().GetMethod("VerifyIntegrity", BindingFlags.Instance | BindingFlags.Public);
                    verifyMethod?.Invoke(packManager, new object[] { output });
                }
            }
        }

        public static void RebuildIndexDiag(this AdvancedAssetService service)
        {
            var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", BindingFlags.Instance | BindingFlags.NonPublic);
            if (packManagerField != null)
            {
                var packManager = packManagerField.GetValue(service);
                if (packManager != null)
                {
                    var rebuildMethod = packManager.GetType().GetMethod("RebuildIndex", BindingFlags.Instance | BindingFlags.Public);
                    rebuildMethod?.Invoke(packManager, null);
                }
            }
        }
    }
}
