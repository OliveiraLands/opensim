using System;
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
    }
}
