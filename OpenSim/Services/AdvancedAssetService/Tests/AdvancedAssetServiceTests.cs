using System;
using System.IO;
using System.Collections.Generic;
using Nini.Config;
using NUnit.Framework;
using OpenMetaverse;
using OpenSim.Framework;
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

        private void WaitForPendingWrites(object packManager)
        {
            var cacheField = packManager.GetType().GetField("m_PendingWritesCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(cacheField, Is.Not.Null);
            var cache = (System.Collections.IDictionary)cacheField.GetValue(packManager);
            Assert.That(cache, Is.Not.Null);

            int retries = 50; // 5 seconds max
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
}
