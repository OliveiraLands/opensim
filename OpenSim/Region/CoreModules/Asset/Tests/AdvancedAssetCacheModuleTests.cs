using System;
using System.IO;
using Nini.Config;
using NUnit.Framework;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.CoreModules.Asset;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;

namespace OpenSim.Region.CoreModules.Asset.Tests
{
    [TestFixture]
    public class AdvancedAssetCacheModuleTests
    {
        private string m_testDir;

        [SetUp]
        public void SetUp()
        {
            m_testDir = Path.Combine(Path.GetTempPath(), "aac_module_test_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(m_testDir);
        }

        [TearDown]
        public void TearDown()
        {
            try
            {
                if (Directory.Exists(m_testDir))
                    Directory.Delete(m_testDir, true);
            }
            catch { }
        }

        private IConfigSource CreateConfig(string cacheDir = null, long maxCacheSize = 2048, long maxPackSize = 256)
        {
            IConfigSource config = new IniConfigSource();
            config.AddConfig("Modules");
            config.Configs["Modules"].Set("AssetCaching", "AdvancedAssetCache");
            config.AddConfig("AssetCache");
            if (cacheDir != null)
                config.Configs["AssetCache"].Set("CacheDirectory", cacheDir);
            config.Configs["AssetCache"].Set("MaxCacheSize", maxCacheSize.ToString());
            config.Configs["AssetCache"].Set("MaxPackSize", maxPackSize.ToString());
            return config;
        }

        private AdvancedAssetCache CreateModule(IConfigSource config = null)
        {
            if (config == null)
                config = CreateConfig(cacheDir: m_testDir);
            AdvancedAssetCache module = new AdvancedAssetCache();
            module.Initialise(config);
            module.PostInitialise();
            return module;
        }

        private AssetBase CreateAsset(UUID id, sbyte type, string name, byte[] data)
        {
            return new AssetBase(id, name, type, UUID.Zero.ToString()) { Data = data };
        }

        [Test]
        public void TestModuleInitialization()
        {
            AdvancedAssetCache module = CreateModule();
            Assert.That(module.Name, Is.EqualTo("AdvancedAssetCache"));
            Assert.That(module.ReplaceableInterface, Is.EqualTo(typeof(IAssetCache)));
            module.Close();
        }

        [Test]
        public void TestModuleNotInitializedWhenDisabled()
        {
            IConfigSource config = new IniConfigSource();
            config.AddConfig("Modules");
            config.Configs["Modules"].Set("AssetCaching", "FlotsamAssetCache");

            AdvancedAssetCache module = new AdvancedAssetCache();
            module.Initialise(config);
            module.PostInitialise();

            Assert.That(module.Name, Is.EqualTo("AdvancedAssetCache"));
            module.Close();
        }

        [Test]
        public void TestCacheAsset()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                AssetBase asset = CreateAsset(id, (sbyte)AssetType.Texture, "Test Texture", data);

                module.Cache(asset);

                AssetBase retrieved = module.Get(id.ToString());
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved.Data, Is.EqualTo(data));
                Assert.That(retrieved.Type, Is.EqualTo(asset.Type));
                Assert.That(retrieved.Name, Is.EqualTo(asset.Name));
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestCacheAssetWithReplace()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                UUID id = UUID.Random();
                byte[] data1 = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                byte[] data2 = new byte[] { 0x05, 0x06, 0x07, 0x08 };

                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "Original", data1), false);
                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "Updated", data2), true);

                AssetBase retrieved = module.Get(id.ToString());
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved.Data, Is.EqualTo(data2));
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestGetNonExistentAsset()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                AssetBase result = module.Get(UUID.Random().ToString());
                Assert.That(result, Is.Null);
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestGetWithEmptyId()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                AssetBase result = module.Get(string.Empty);
                Assert.That(result, Is.Null);
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestCheckAssetExists()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };

                Assert.That(module.Check(id.ToString()), Is.False);

                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "CheckTest", data));

                Assert.That(module.Check(id.ToString()), Is.True);
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestGetWithOutParameter()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "OutParam", data));

                AssetBase asset;
                bool result = module.Get(id.ToString(), out asset);
                Assert.That(result, Is.True);
                Assert.That(asset, Is.Not.Null);
                Assert.That(asset.Data, Is.EqualTo(data));
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestCacheNegativeIsNoOp()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                module.CacheNegative("some-id");
                Assert.That(module.Check("some-id"), Is.False);
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestClearCache()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "ClearTest", data));

                module.Clear();

                Assert.That(module.Check(id.ToString()), Is.False);
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestExpireIsNoOp()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "ExpireTest", data));

                module.Expire(id.ToString());

                Assert.That(module.Check(id.ToString()), Is.True);
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestCustomConfiguration()
        {
            string customDir = Path.Combine(m_testDir, "custom_cache");
            IConfigSource config = CreateConfig(cacheDir: customDir, maxCacheSize: 512, maxPackSize: 64);
            AdvancedAssetCache module = new AdvancedAssetCache();
            module.Initialise(config);
            module.PostInitialise();

            try
            {
                Assert.That(Directory.Exists(customDir), Is.True);

                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "CustomConfig", data));

                AssetBase retrieved = module.Get(id.ToString());
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved.Data, Is.EqualTo(data));
            }
            finally
            {
                module.Close();
            }
        }

        [Test]
        public void TestMultipleAssetsCachedAndRetrieved()
        {
            AdvancedAssetCache module = CreateModule();
            try
            {
                List<string> ids = new List<string>();
                for (int i = 0; i < 50; i++)
                {
                    UUID id = UUID.Random();
                    ids.Add(id.ToString());
                    byte[] data = BitConverter.GetBytes(i);
                    module.Cache(CreateAsset(id, (sbyte)AssetType.Texture, "Multi " + i, data));
                }

                foreach (string id in ids)
                {
                    Assert.That(module.Check(id), Is.True,
                        "Asset {0} should be in cache", id);
                }
            }
            finally
            {
                module.Close();
            }
        }
    }
}
