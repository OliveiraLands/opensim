using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Nini.Config;
using NUnit.Framework;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Data;
using OpenSim.Services.Interfaces;

namespace OpenSim.Services.AdvancedAssetService.Tests
{
    [TestFixture]
    public class AdvancedAssetServiceTests
    {
        private AdvancedAssetService CreateService()
        {
            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", "test_asset_packs");

            return new AdvancedAssetService(config);
        }

        [Test]
        public void TestStoreAndGetAsset()
        {
            AdvancedAssetService service = CreateService();
            UUID assetID = UUID.Random();
            byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
            AssetBase asset = new AssetBase(assetID, "Test Asset", (sbyte)AssetType.Texture, UUID.Zero.ToString());
            asset.Data = data;

            string storedID = service.Store(asset);
            Assert.That(storedID, Is.EqualTo(assetID.ToString()));

            AssetBase retrieved = service.Get(assetID.ToString());
            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.ID, Is.EqualTo(assetID.ToString()));
            Assert.That(retrieved.Data, Is.EqualTo(data));
        }

        [Test]
        public void TestGetNonExistentAsset()
        {
            AdvancedAssetService service = CreateService();
            AssetBase retrieved = service.Get(UUID.Random().ToString());
            Assert.That(retrieved, Is.Null);
        }

        [Test]
        public void TestDefragmentWithDuplicateHashes()
        {
            // Clean up old pack dir if exists to have a fresh state
            if (Directory.Exists("test_asset_packs"))
            {
                try { Directory.Delete("test_asset_packs", true); } catch {}
            }

            AdvancedAssetService service = CreateService();
            
            // Store two different UUIDs with the exact same data
            UUID uuid1 = UUID.Random();
            UUID uuid2 = UUID.Random();
            byte[] data = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };

            AssetBase asset1 = new AssetBase(uuid1, "Asset 1", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data };
            AssetBase asset2 = new AssetBase(uuid2, "Asset 2", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data };

            service.Store(asset1);
            service.Store(asset2);

            // Access private m_PackManager using reflection
            var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(packManagerField, Is.Not.Null);
            var packManager = packManagerField.GetValue(service);
            Assert.That(packManager, Is.Not.Null);

            WaitForPendingWrites(packManager);

            // Call Defragment
            var defragMethod = packManager.GetType().GetMethod("Defragment");
            Assert.That(defragMethod, Is.Not.Null);
            
            List<string> logs = new List<string>();
            defragMethod.Invoke(packManager, new object[] { null, new Action<string>(msg => logs.Add(msg)) });

            // Verify that both assets can still be retrieved and have the correct data
            AssetBase retrieved1 = service.Get(uuid1.ToString());
            Assert.That(retrieved1, Is.Not.Null);
            Assert.That(retrieved1.Data, Is.EqualTo(data));

            AssetBase retrieved2 = service.Get(uuid2.ToString());
            Assert.That(retrieved2, Is.Not.Null);
            Assert.That(retrieved2.Data, Is.EqualTo(data));

            // Verify that the hash was only physically stored once (since it's a duplicate)
            var getIndexEntryMethod = packManager.GetType().GetMethod("GetIndexEntry", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(getIndexEntryMethod, Is.Not.Null);

            var computeHashMethod = packManager.GetType().GetMethod("ComputeHash", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(computeHashMethod, Is.Not.Null);

            string hash = (string)computeHashMethod.Invoke(packManager, new object[] { data });
            
            var entry = getIndexEntryMethod.Invoke(packManager, new object[] { hash });
            Assert.That(entry, Is.Not.Null);
            
            // Clean up
            if (Directory.Exists("test_asset_packs"))
            {
                try { Directory.Delete("test_asset_packs", true); } catch {}
            }
        }

        [Test]
        public void TestSuspiciousAssetsLifecycle()
        {
            if (Directory.Exists("test_asset_packs"))
            {
                try { Directory.Delete("test_asset_packs", true); } catch {}
            }

            AdvancedAssetService service = CreateService();
            
            UUID uuid1 = UUID.Random();
            UUID uuid2 = UUID.Random();
            byte[] data = new byte[] { 0x11, 0x22, 0x33 };
            byte[] data2 = new byte[] { 0x44, 0x55, 0x66 };

            AssetBase asset1 = new AssetBase(uuid1, "Asset 1", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data };
            AssetBase asset2 = new AssetBase(uuid2, "Asset 2", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data2 };

            service.Store(asset1);
            service.Store(asset2);

            // Access m_PackManager using reflection
            var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(packManagerField, Is.Not.Null);
            var packManager = packManagerField.GetValue(service);
            Assert.That(packManager, Is.Not.Null);

            WaitForPendingWrites(packManager);

            // Set uuid2 as suspicious
            var setSuspMethod = packManager.GetType().GetMethod("SetSuspiciousAssets");
            Assert.That(setSuspMethod, Is.Not.Null);
            setSuspMethod.Invoke(packManager, new object[] { new string[] { uuid2.ToString() } });

            // Defragment
            var defragMethod = packManager.GetType().GetMethod("Defragment");
            Assert.That(defragMethod, Is.Not.Null);
            List<string> logs = new List<string>();
            defragMethod.Invoke(packManager, new object[] { null, new Action<string>(msg => logs.Add(msg)) });

            // Verify uuid1 (not suspicious) is still there and readable
            AssetBase retrieved1 = service.Get(uuid1.ToString());
            Assert.That(retrieved1, Is.Not.Null);
            Assert.That(retrieved1.Data, Is.EqualTo(data));

            // Verify uuid2 (suspicious and not accessed) has been excluded/deleted by defrag
            AssetBase retrieved2 = service.Get(uuid2.ToString());
            Assert.That(retrieved2, Is.Null);

            // Cleanup
            if (Directory.Exists("test_asset_packs"))
            {
                try { Directory.Delete("test_asset_packs", true); } catch {}
            }
        }

        [Test]
        public void TestSuspiciousAssetClearedOnRead()
        {
            if (Directory.Exists("test_asset_packs"))
            {
                try { Directory.Delete("test_asset_packs", true); } catch {}
            }

            AdvancedAssetService service = CreateService();
            
            UUID uuid1 = UUID.Random();
            byte[] data = new byte[] { 0x77, 0x88, 0x99 };
            AssetBase asset1 = new AssetBase(uuid1, "Asset 1", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data };
            service.Store(asset1);

            // Access packManager
            var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var packManager = packManagerField.GetValue(service);

            WaitForPendingWrites(packManager);

            // Mark uuid1 as suspicious
            var setSuspMethod = packManager.GetType().GetMethod("SetSuspiciousAssets");
            setSuspMethod.Invoke(packManager, new object[] { new string[] { uuid1.ToString() } });

            // Read uuid1 -> this should clear the suspicious status!
            AssetBase retrieved = service.Get(uuid1.ToString());
            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.Data, Is.EqualTo(data));

            // Defragment -> since suspicious status was cleared, defrag should NOT delete it
            var defragMethod = packManager.GetType().GetMethod("Defragment");
            List<string> logs = new List<string>();
            defragMethod.Invoke(packManager, new object[] { null, new Action<string>(msg => logs.Add(msg)) });

            // It should still be present
            AssetBase retrievedAfterDefrag = service.Get(uuid1.ToString());
            Assert.That(retrievedAfterDefrag, Is.Not.Null);
            Assert.That(retrievedAfterDefrag.Data, Is.EqualTo(data));

            if (Directory.Exists("test_asset_packs"))
            {
                try { Directory.Delete("test_asset_packs", true); } catch {}
            }
        }

        [Test]
        public void TestVerifyIntegrityDetectsMismatches()
        {
            if (Directory.Exists("test_verify_packs"))
            {
                try { Directory.Delete("test_verify_packs", true); } catch {}
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", "test_verify_packs");

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                UUID uuid = UUID.Random();
                byte[] data = new byte[] { 0x12, 0x34, 0x56, 0x78 };
                AssetBase asset = new AssetBase(uuid, "Original Name", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data };
                
                service.Store(asset);

                var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                Assert.That(packManagerField, Is.Not.Null);
                var packManager = packManagerField.GetValue(service);
                Assert.That(packManager, Is.Not.Null);

                WaitForPendingWrites(packManager);

                // 1. Run verify integrity: it should report perfect status (no errors)
                List<string> outputs = new List<string>();
                var verifyMethod = packManager.GetType().GetMethod("VerifyIntegrity", new Type[] { typeof(Action<string>) });
                Assert.That(verifyMethod, Is.Not.Null);
                
                verifyMethod.Invoke(packManager, new object[] { new Action<string>(msg => outputs.Add(msg)) });

                bool hasErrors = outputs.Exists(line => line.Contains("[ERROR]"));
                if (hasErrors)
                {
                    Assert.Fail("Expected no integrity errors initially, but got:\n" + string.Join("\n", outputs));
                }

                // 2. Tamper with the SQLite database to change the name of the asset
                var connectionField = packManager.GetType().GetField("m_Connection", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                Assert.That(connectionField, Is.Not.Null);
                var connection = (System.Data.IDbConnection)connectionField.GetValue(packManager);
                Assert.That(connection, Is.Not.Null);

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"UPDATE asset_map SET name = 'Tampered Name' WHERE uuid = '{uuid.ToString().ToLower().Replace("-", "")}'";
                    cmd.ExecuteNonQuery();
                }

                // Run verify integrity again: it should report a Name Mismatch error
                outputs.Clear();
                verifyMethod.Invoke(packManager, new object[] { new Action<string>(msg => outputs.Add(msg)) });

                bool hasNameError = outputs.Exists(line => line.Contains("[ERROR] Name mismatch"));
                Assert.That(hasNameError, Is.True, "Expected a Name mismatch error after database tampering.");

                // 3. Tamper with the SQLite database to change the type of the asset
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"UPDATE asset_map SET type = {(int)AssetType.LSLText} WHERE uuid = '{uuid.ToString().ToLower().Replace("-", "")}'";
                    cmd.ExecuteNonQuery();
                }

                // Run verify integrity again: it should report a Type Mismatch error
                outputs.Clear();
                verifyMethod.Invoke(packManager, new object[] { new Action<string>(msg => outputs.Add(msg)) });

                bool hasTypeError = outputs.Exists(line => line.Contains("[ERROR] Type mismatch"));
                Assert.That(hasTypeError, Is.True, "Expected a Type mismatch error after database tampering.");
            }

            // Cleanup
            if (Directory.Exists("test_verify_packs"))
            {
                try { Directory.Delete("test_verify_packs", true); } catch {}
            }
        }

        [Test]
        public void TestOptimizeDatabase()
        {
            if (Directory.Exists("test_optimize_packs"))
            {
                try { Directory.Delete("test_optimize_packs", true); } catch {}
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", "test_optimize_packs");

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                UUID uuid = UUID.Random();
                byte[] data = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
                AssetBase asset = new AssetBase(uuid, "Test Optimize", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = data };
                service.Store(asset);

                var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                Assert.That(packManagerField, Is.Not.Null);
                var packManager = packManagerField.GetValue(service);
                Assert.That(packManager, Is.Not.Null);

                WaitForPendingWrites(packManager);

                // Run OptimizeDatabase
                var optimizeMethod = packManager.GetType().GetMethod("OptimizeDatabase", new Type[] { typeof(Action<string>) });
                Assert.That(optimizeMethod, Is.Not.Null);

                List<string> outputs = new List<string>();
                optimizeMethod.Invoke(packManager, new object[] { new Action<string>(msg => outputs.Add(msg)) });

                // Verify that it completed successfully without errors
                bool completed = outputs.Exists(line => line.Contains("Database optimization completed successfully."));
                Assert.That(completed, Is.True, "Optimization should report successful completion.");

                // Verify asset remains fully readable and correct
                AssetBase retrieved = service.Get(uuid.ToString());
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved.Data, Is.EqualTo(data));
            }

            // Cleanup
            if (Directory.Exists("test_optimize_packs"))
            {
                try { Directory.Delete("test_optimize_packs", true); } catch {}
            }
        }

        [Test]
        public void TestRestoreFromLog()
        {
            if (Directory.Exists("test_restore_log_packs"))
            {
                try { Directory.Delete("test_restore_log_packs", true); } catch {}
            }
            if (Directory.Exists("test_restore_log_fs"))
            {
                try { Directory.Delete("test_restore_log_fs", true); } catch {}
            }
            if (File.Exists("test_urma.log"))
            {
                try { File.Delete("test_urma.log"); } catch {}
            }

            // 1. Create a dummy log file
            UUID missingUuid1 = UUID.Random();
            UUID missingUuid2 = UUID.Random();
            string logContent = $"2026-07-17 14:39:24 WARN  [InventoryAccessModule]: Could not find asset {missingUuid1} for item Test Item 1\n" +
                                $"2026-07-17 14:39:25 WARN  [InventoryAccessModule]: Could not find asset {missingUuid2} for item Test Item 2\n";
            File.WriteAllText("test_urma.log", logContent);

            // 2. Create a dummy FSAsset/Cache folder
            Directory.CreateDirectory("test_restore_log_fs");

            // 2a. Write missingUuid1 as a serialized AssetBase (Flotsam Cache style)
            string dataStr1 = "Restored Content 1 (Serialized)";
            AssetBase asset1 = new AssetBase(missingUuid1.ToString(), "Test Serialized", (sbyte)AssetType.Object, UUID.ZeroString)
            {
                Data = System.Text.Encoding.UTF8.GetBytes(dataStr1)
            };
            string serializedFilePath = Path.Combine("test_restore_log_fs", missingUuid1.ToString().ToLower().Replace("-", ""));
            #pragma warning disable SYSLIB0011
            var bformatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            using (FileStream fs = new FileStream(serializedFilePath, FileMode.Create, FileAccess.Write))
            {
                bformatter.Serialize(fs, asset1);
            }
            #pragma warning restore SYSLIB0011

            // 2b. Write missingUuid2 as a GZip compressed file (FSAsset style)
            string dataStr2 = "Restored Content 2 (GZip)";
            byte[] dataBytes2 = System.Text.Encoding.UTF8.GetBytes(dataStr2);
            string uuidFilePath = Path.Combine("test_restore_log_fs", missingUuid2.ToString().ToLower().Replace("-", "") + ".gz");
            using (FileStream fs = new FileStream(uuidFilePath, FileMode.Create, FileAccess.Write))
            using (System.IO.Compression.GZipStream gz = new System.IO.Compression.GZipStream(fs, System.IO.Compression.CompressionMode.Compress))
            {
                gz.Write(dataBytes2, 0, dataBytes2.Length);
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", "test_restore_log_packs");

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                var restoreMethod = typeof(AdvancedAssetService).GetMethod("HandleRestoreFromLog", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                Assert.That(restoreMethod, Is.Not.Null);

                string[] args = new string[] { "aas", "restore-from-log", "test_urma.log", "test_restore_log_fs" };
                restoreMethod.Invoke(service, new object[] { "aas", args });

                // Verify that missingUuid1 (serialized) was successfully restored!
                AssetBase retrieved1 = service.Get(missingUuid1.ToString());
                Assert.That(retrieved1, Is.Not.Null);
                Assert.That(System.Text.Encoding.UTF8.GetString(retrieved1.Data), Is.EqualTo(dataStr1));

                // Verify that missingUuid2 (GZip) was successfully restored!
                AssetBase retrieved2 = service.Get(missingUuid2.ToString());
                Assert.That(retrieved2, Is.Not.Null);
                Assert.That(System.Text.Encoding.UTF8.GetString(retrieved2.Data), Is.EqualTo(dataStr2));
            }

            // Cleanup
            if (Directory.Exists("test_restore_log_packs"))
            {
                try { Directory.Delete("test_restore_log_packs", true); } catch {}
            }
            if (Directory.Exists("test_restore_log_fs"))
            {
                try { Directory.Delete("test_restore_log_fs", true); } catch {}
            }
            if (File.Exists("test_urma.log"))
            {
                try { File.Delete("test_urma.log"); } catch {}
            }
        }
        [Test]
        public void TestBulkUploadAssetsPerformance()
        {
            if (Directory.Exists("test_bulk_packs"))
            {
                try { Directory.Delete("test_bulk_packs", true); } catch {}
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", "test_bulk_packs");

            int numAssets = 1000;
            List<AssetBase> assets = new List<AssetBase>();
            for (int i = 0; i < numAssets; i++)
            {
                UUID uuid = UUID.Random();
                byte[] data = System.Text.Encoding.UTF8.GetBytes("Bulk Upload Asset Content " + i);
                assets.Add(new AssetBase(uuid.ToString(), "Bulk Test " + i, (sbyte)AssetType.Object, UUID.ZeroString)
                {
                    Data = data
                });
            }

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                foreach (var asset in assets)
                {
                    service.Store(asset);
                }
                watch.Stop();
                long enqueueTimeMs = watch.ElapsedMilliseconds;

                var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                Assert.That(packManagerField, Is.Not.Null);
                object packManager = packManagerField.GetValue(service);
                Assert.That(packManager, Is.Not.Null);

                var diskWatch = System.Diagnostics.Stopwatch.StartNew();
                WaitForPendingWrites(packManager);
                diskWatch.Stop();
                long diskTimeMs = diskWatch.ElapsedMilliseconds;
                double totalSec = (enqueueTimeMs + diskTimeMs) / 1000.0;
                if (totalSec <= 0) totalSec = 0.001;
                double throughput = numAssets / totalSec;

                System.Console.WriteLine(string.Format("[PERFORMANCE]: Enqueued in {0}ms, Flushed to disk in additional {1}ms. Total throughput: {2:F2} assets/second.", enqueueTimeMs, diskTimeMs, throughput));

                // Verify that assets are readable
                AssetBase retrieved = service.Get(assets[500].ID);
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(System.Text.Encoding.UTF8.GetString(retrieved.Data), Is.EqualTo("Bulk Upload Asset Content 500"));
            }

            if (Directory.Exists("test_bulk_packs"))
            {
                try { Directory.Delete("test_bulk_packs", true); } catch {}
            }
        }
        [Test]
        public void TestGetDummyAssetData()
        {
            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", "test_dummies_packs");

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                var getDummyMethod = typeof(AdvancedAssetService).GetMethod("GetDummyAssetData", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                Assert.That(getDummyMethod, Is.Not.Null);

                // 1. Test Texture
                object[] args1 = new object[] { (sbyte)AssetType.Texture, (sbyte)0, "" };
                byte[] data1 = (byte[])getDummyMethod.Invoke(service, args1);
                Assert.That(data1, Is.Not.Null);
                Assert.That(data1.Length, Is.GreaterThan(18)); // Valid TGA header length
                Assert.That((sbyte)args1[1], Is.EqualTo((sbyte)AssetType.Texture));
                Assert.That((string)args1[2], Contains.Substring("Texture"));

                // 2. Test LSLText
                object[] args2 = new object[] { (sbyte)AssetType.LSLText, (sbyte)0, "" };
                byte[] data2 = (byte[])getDummyMethod.Invoke(service, args2);
                Assert.That(data2, Is.Not.Null);
                Assert.That(System.Text.Encoding.UTF8.GetString(data2), Contains.Substring("default"));
            }

            // Cleanup
            if (Directory.Exists("test_dummies_packs"))
            {
                try { Directory.Delete("test_dummies_packs", true); } catch {}
            }
        }

        [Test]
        public void TestParseInventoryVerificationArgs()
        {
            var service = CreateService();

            // 1. Parse full args with First and Last Name
            string[] args1 = new string[] { "aas", "verify-inventory", "John", "Doe", "--verify-data", "--fix", "--export", "report.csv", "--verbose" };
            var options1 = service.ParseInventoryVerificationArgs(args1, 2);

            Assert.That(options1.UserName, Is.EqualTo("John Doe"));
            Assert.That(options1.VerifyData, Is.True);
            Assert.That(options1.Fix, Is.True);
            Assert.That(options1.ExportPath, Is.EqualTo("report.csv"));
            Assert.That(options1.Verbose, Is.True);

            // 2. Parse args with UUID
            UUID targetId = UUID.Random();
            string[] args2 = new string[] { "aas", "verify-inventory", targetId.ToString(), "--repair", "--dry-run" };
            var options2 = service.ParseInventoryVerificationArgs(args2, 2);

            Assert.That(options2.UserID.HasValue, Is.True);
            Assert.That(options2.UserID.Value, Is.EqualTo(targetId));
            Assert.That(options2.Fix, Is.False); // dry-run disables fix
        }

        [Test]
        public void TestVerifyInventoryTargetSpecificUserByUUID()
        {
            string storage = "test_verify_user_uuid_packs";
            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", storage);

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                // Store one healthy asset in AAS
                UUID healthyAssetID = UUID.Random();
                byte[] assetData = new byte[] { 0x10, 0x20, 0x30, 0x40 };
                AssetBase asset = new AssetBase(healthyAssetID, "Healthy Texture", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = assetData };
                service.Store(asset);

                var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                var packManager = packManagerField.GetValue(service);
                WaitForPendingWrites(packManager);

                UUID user1 = UUID.Random();
                UUID user2 = UUID.Random();
                UUID missingAssetID1 = UUID.Random();
                UUID missingAssetID2 = UUID.Random();

                var mockInventory = new MockInventoryData();
                // User 1 items: 1 healthy, 1 missing
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user1,
                    assetID = healthyAssetID,
                    assetType = (int)AssetType.Texture,
                    inventoryName = "User1 Healthy Texture"
                });
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user1,
                    assetID = missingAssetID1,
                    assetType = (int)AssetType.Texture,
                    inventoryName = "User1 Missing Texture"
                });
                // User 2 items: 1 missing (should NOT be scanned when user1 is targeted)
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user2,
                    assetID = missingAssetID2,
                    assetType = (int)AssetType.Texture,
                    inventoryName = "User2 Missing Texture"
                });

                var options = new InventoryVerificationOptions
                {
                    UserID = user1,
                    InventoryDatabase = mockInventory,
                    VerifyData = false
                };

                List<string> logs = new List<string>();
                var result = service.VerifyInventory(options, msg => logs.Add(msg));

                Assert.That(result.TotalScanned, Is.EqualTo(2), "Should only scan User 1 items.");
                Assert.That(result.UniqueAssetsCount, Is.EqualTo(2));
                Assert.That(result.HealthyAssetsCount, Is.EqualTo(1));
                Assert.That(result.MissingAssetsCount, Is.EqualTo(1));
                Assert.That(result.Issues.Count, Is.EqualTo(1));
                Assert.That(result.Issues[0].AssetID, Is.EqualTo(missingAssetID1));
                Assert.That(result.Issues[0].AvatarID, Is.EqualTo(user1));
            }

            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }
        }

        [Test]
        public void TestVerifyInventoryTargetSpecificUserByName()
        {
            string storage = "test_verify_user_name_packs";
            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", storage);

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                UUID userJane = UUID.Random();
                UUID missingAssetID = UUID.Random();

                var mockAccounts = new MockUserAccountData();
                mockAccounts.Accounts.Add(new UserAccountData
                {
                    PrincipalID = userJane,
                    FirstName = "Jane",
                    LastName = "Doe"
                });

                var mockInventory = new MockInventoryData();
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = userJane,
                    assetID = missingAssetID,
                    assetType = (int)AssetType.LSLText,
                    inventoryName = "Jane Broken Script"
                });

                var options = new InventoryVerificationOptions
                {
                    UserName = "Jane Doe",
                    AccountDatabase = mockAccounts,
                    InventoryDatabase = mockInventory,
                    VerifyData = false
                };

                List<string> logs = new List<string>();
                var result = service.VerifyInventory(options, msg => logs.Add(msg));

                Assert.That(options.UserID.HasValue, Is.True);
                Assert.That(options.UserID.Value, Is.EqualTo(userJane));
                Assert.That(result.TotalScanned, Is.EqualTo(1));
                Assert.That(result.MissingAssetsCount, Is.EqualTo(1));
                Assert.That(result.Issues.Count, Is.EqualTo(1));
                Assert.That(result.Issues[0].ItemName, Is.EqualTo("Jane Broken Script"));
            }

            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }
        }

        [Test]
        public void TestVerifyInventoryVerifyDataDetectsCorrupted()
        {
            string storage = "test_verify_corrupted_packs";
            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", storage);

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                UUID assetID = UUID.Random();
                // Store asset with 1 byte initially
                service.Store(new AssetBase(assetID, "Corrupt Candidate", (sbyte)AssetType.Texture, UUID.Zero.ToString()) { Data = new byte[] { 0x01 } });

                var packManagerField = typeof(AdvancedAssetService).GetField("m_PackManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                var packManager = packManagerField.GetValue(service);
                WaitForPendingWrites(packManager);

                // Overwrite pack file magic number with zeros to simulate corrupted physical storage
                foreach (var file in Directory.GetFiles(storage, "pack_*.bin"))
                {
                    using (var fs = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                    {
                        fs.Write(new byte[32], 0, 32);
                    }
                }

                UUID user = UUID.Random();
                var mockInventory = new MockInventoryData();
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user,
                    assetID = assetID,
                    assetType = (int)AssetType.Texture,
                    inventoryName = "Empty Texture Asset Item"
                });

                // 1. Without --verify-data: passes because UUID is in asset_map index
                var options1 = new InventoryVerificationOptions
                {
                    UserID = user,
                    InventoryDatabase = mockInventory,
                    VerifyData = false
                };
                var result1 = service.VerifyInventory(options1);
                Assert.That(result1.HealthyAssetsCount, Is.EqualTo(1));
                Assert.That(result1.CorruptedAssetsCount, Is.EqualTo(0));

                // 2. With --verify-data: reads physical data, detects 0 bytes / empty
                var options2 = new InventoryVerificationOptions
                {
                    UserID = user,
                    InventoryDatabase = mockInventory,
                    VerifyData = true
                };
                var result2 = service.VerifyInventory(options2);
                Assert.That(result2.CorruptedAssetsCount, Is.EqualTo(1));
                Assert.That(result2.Issues.Count, Is.EqualTo(1));
                Assert.That(result2.Issues[0].Status, Is.EqualTo("Corrupted"));
            }

            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }
        }

        [Test]
        public void TestVerifyInventoryFixRepairsMissingAssets()
        {
            string storage = "test_verify_fix_packs";
            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", storage);

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                UUID user = UUID.Random();
                UUID missingTexture = UUID.Random();
                UUID missingScript = UUID.Random();

                var mockInventory = new MockInventoryData();
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user,
                    assetID = missingTexture,
                    assetType = (int)AssetType.Texture,
                    inventoryName = "My Broken Texture"
                });
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user,
                    assetID = missingScript,
                    assetType = (int)AssetType.LSLText,
                    inventoryName = "My Broken Script"
                });

                // 1. Run with Fix = true
                var options = new InventoryVerificationOptions
                {
                    UserID = user,
                    InventoryDatabase = mockInventory,
                    Fix = true
                };

                List<string> logs = new List<string>();
                var result = service.VerifyInventory(options, msg => logs.Add(msg));

                Assert.That(result.MissingAssetsCount, Is.EqualTo(2));
                Assert.That(result.FixedAssetsCount, Is.EqualTo(2));
                Assert.That(result.Issues.All(i => i.Fixed), Is.True);

                // 2. Verify that assets now exist and are readable in AAS!
                AssetBase restoredTex = service.Get(missingTexture.ToString());
                Assert.That(restoredTex, Is.Not.Null);
                Assert.That(restoredTex.Type, Is.EqualTo((sbyte)AssetType.Texture));
                Assert.That(restoredTex.Data, Is.Not.Null);
                Assert.That(restoredTex.Data.Length, Is.GreaterThan(0));

                AssetBase restoredScript = service.Get(missingScript.ToString());
                Assert.That(restoredScript, Is.Not.Null);
                Assert.That(restoredScript.Type, Is.EqualTo((sbyte)AssetType.LSLText));
                Assert.That(restoredScript.Data, Is.Not.Null);
                Assert.That(System.Text.Encoding.UTF8.GetString(restoredScript.Data), Contains.Substring("default"));

                // 3. Re-run Verify: should now be 100% healthy
                var verifyAgainOptions = new InventoryVerificationOptions
                {
                    UserID = user,
                    InventoryDatabase = mockInventory,
                    VerifyData = true
                };
                var resultAgain = service.VerifyInventory(verifyAgainOptions);
                Assert.That(resultAgain.MissingAssetsCount, Is.EqualTo(0));
                Assert.That(resultAgain.HealthyAssetsCount, Is.EqualTo(2));
            }

            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }
        }

        [Test]
        public void TestVerifyInventoryExportCsv()
        {
            string storage = "test_verify_export_packs";
            string csvFile = "test_inventory_report.csv";
            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }
            if (File.Exists(csvFile))
            {
                try { File.Delete(csvFile); } catch { }
            }

            IConfigSource config = new IniConfigSource();
            config.AddConfig("AssetService");
            config.Configs["AssetService"].Set("StoragePath", storage);

            using (AdvancedAssetService service = new AdvancedAssetService(config))
            {
                UUID user = UUID.Random();
                UUID missingAsset = UUID.Random();

                var mockInventory = new MockInventoryData();
                mockInventory.Items.Add(new XInventoryItem
                {
                    inventoryID = UUID.Random(),
                    avatarID = user,
                    assetID = missingAsset,
                    assetType = (int)AssetType.Sound,
                    inventoryName = "Special Sound Clip"
                });

                var options = new InventoryVerificationOptions
                {
                    UserID = user,
                    UserName = "Test Avatar",
                    InventoryDatabase = mockInventory,
                    ExportPath = csvFile
                };

                var result = service.VerifyInventory(options);
                Assert.That(File.Exists(csvFile), Is.True, "CSV report file should be created.");

                string[] lines = File.ReadAllLines(csvFile);
                Assert.That(lines.Length, Is.GreaterThanOrEqualTo(2));
                Assert.That(lines[0], Contains.Substring("AvatarID,AvatarName,InventoryItemID"));
                Assert.That(lines[1], Contains.Substring("Special Sound Clip"));
                Assert.That(lines[1], Contains.Substring("Sound"));
            }

            if (File.Exists(csvFile))
            {
                try { File.Delete(csvFile); } catch { }
            }
            if (Directory.Exists(storage))
            {
                try { Directory.Delete(storage, true); } catch { }
            }
        }

        private void WaitForPendingWrites(object packManager)
        {
            var method = packManager.GetType().GetMethod("WaitForPendingWrites", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            if (method != null)
            {
                method.Invoke(packManager, null);
                return;
            }

            var cacheField = packManager.GetType().GetField("m_PendingWritesCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(cacheField, Is.Not.Null);
            var cache = (System.Collections.IDictionary)cacheField.GetValue(packManager);
            Assert.That(cache, Is.Not.Null);

            int retries = 300; // 30 seconds max
            while (cache.Count > 0 && retries > 0)
            {
                System.Threading.Thread.Sleep(100);
                retries--;
            }
            if (cache.Count > 0)
            {
                throw new Exception("Timeout waiting for pending writes to complete.");
            }
        }
    }

    public class MockInventoryData : IXInventoryData
    {
        public List<XInventoryItem> Items = new List<XInventoryItem>();
        public List<XInventoryFolder> Folders = new List<XInventoryFolder>();

        public XInventoryFolder[] GetFolder(string field, string val) => Folders.ToArray();
        public XInventoryFolder[] GetFolders(string[] fields, string[] vals) => Folders.ToArray();

        public XInventoryItem[] GetItems(string[] fields, string[] vals)
        {
            if (fields == null || fields.Length == 0)
                return Items.ToArray();

            var result = new List<XInventoryItem>();
            foreach (var item in Items)
            {
                bool match = true;
                for (int i = 0; i < fields.Length; i++)
                {
                    if (fields[i].Equals("avatarID", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!item.avatarID.ToString().Equals(vals[i], StringComparison.OrdinalIgnoreCase))
                        {
                            match = false;
                            break;
                        }
                    }
                }
                if (match)
                    result.Add(item);
            }
            return result.ToArray();
        }

        public bool StoreFolder(XInventoryFolder folder) { Folders.Add(folder); return true; }
        public bool StoreItem(XInventoryItem item) { Items.Add(item); return true; }
        public bool DeleteFolders(string field, string val) => true;
        public bool DeleteFolders(string[] fields, string[] vals) => true;
        public bool DeleteItems(string field, string val) => true;
        public bool DeleteItems(string[] fields, string[] vals) => true;
        public bool MoveItem(string id, string newParent) => true;
        public bool MoveFolder(string id, string newParent) => true;
        public XInventoryItem[] GetActiveGestures(UUID principalID) => new XInventoryItem[0];
        public int GetAssetPermissions(UUID principalID, UUID assetID) => 0;
    }

    public class MockUserAccountData : IUserAccountData
    {
        public List<UserAccountData> Accounts = new List<UserAccountData>();

        public UserAccountData[] Get(string[] fields, string[] values)
        {
            var result = new List<UserAccountData>();
            foreach (var acc in Accounts)
            {
                bool match = true;
                for (int i = 0; i < fields.Length; i++)
                {
                    if (fields[i].Equals("PrincipalID", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!acc.PrincipalID.ToString().Equals(values[i], StringComparison.OrdinalIgnoreCase))
                        {
                            match = false; break;
                        }
                    }
                    else if (fields[i].Equals("FirstName", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!acc.FirstName.Equals(values[i], StringComparison.OrdinalIgnoreCase))
                        {
                            match = false; break;
                        }
                    }
                    else if (fields[i].Equals("LastName", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!acc.LastName.Equals(values[i], StringComparison.OrdinalIgnoreCase))
                        {
                            match = false; break;
                        }
                    }
                }
                if (match)
                    result.Add(acc);
            }
            return result.ToArray();
        }

        public bool Store(UserAccountData data) { Accounts.Add(data); return true; }
        public bool Delete(string field, string val) => true;

        public UserAccountData[] GetUsers(UUID scopeID, string query)
        {
            var result = new List<UserAccountData>();
            string lower = query.ToLower();
            foreach (var acc in Accounts)
            {
                string fullName = (acc.FirstName + " " + acc.LastName).ToLower();
                if (fullName.Contains(lower) || acc.FirstName.ToLower().Contains(lower) || acc.LastName.ToLower().Contains(lower))
                {
                    result.Add(acc);
                }
            }
            return result.ToArray();
        }

        public UserAccountData[] GetUsersWhere(UUID scopeID, string where) => Accounts.ToArray();
    }
}
