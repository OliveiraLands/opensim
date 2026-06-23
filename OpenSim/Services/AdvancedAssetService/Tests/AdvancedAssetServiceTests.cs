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
    }
}
