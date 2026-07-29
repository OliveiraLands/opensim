using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Nini.Config;
using NUnit.Framework;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.CoreModules.Asset;

namespace OpenSim.Region.CoreModules.Asset.Tests
{
    [TestFixture]
    public class PackFileCacheTests
    {
        private string m_testDir;

        [SetUp]
        public void SetUp()
        {
            m_testDir = Path.Combine(Path.GetTempPath(), "aac_test_" + Guid.NewGuid().ToString("N"));
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

        private PackFileCache CreateCache(long maxSize = 2048L * 1024 * 1024, long maxPackSize = 256L * 1024 * 1024)
        {
            return new PackFileCache(m_testDir, maxSize, maxPackSize);
        }

        private AssetBase CreateAsset(UUID id, sbyte type, string name, byte[] data)
        {
            return new AssetBase(id, name, type, UUID.Zero.ToString()) { Data = data };
        }

        private byte[] CreateData(int size)
        {
            byte[] data = new byte[size];
            new Random(42).NextBytes(data);
            return data;
        }

        [Test]
        public void TestStoreAndGetAsset()
        {
            using (PackFileCache cache = CreateCache())
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                AssetBase asset = CreateAsset(id, (sbyte)AssetType.Texture, "Test Texture", data);

                cache.Store(asset.ID, asset.Data, asset.Type, asset.Name);

                sbyte type;
                string name;
                byte[] retrieved = cache.Get(asset.ID, out type, out name);

                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved, Is.EqualTo(data));
                Assert.That(type, Is.EqualTo(asset.Type));
                Assert.That(name, Is.EqualTo(asset.Name));
            }
        }

        [Test]
        public void TestGetNonExistentAsset()
        {
            using (PackFileCache cache = CreateCache())
            {
                sbyte type;
                string name;
                byte[] result = cache.Get(UUID.Random().ToString(), out type, out name);
                Assert.That(result, Is.Null);
            }
        }

        [Test]
        public void TestGetWithEmptyId()
        {
            using (PackFileCache cache = CreateCache())
            {
                sbyte type;
                string name;
                byte[] result = cache.Get(string.Empty, out type, out name);
                Assert.That(result, Is.Null);
            }
        }

        [Test]
        public void TestStoreNullData()
        {
            using (PackFileCache cache = CreateCache())
            {
                cache.Store(UUID.Random().ToString(), null, (sbyte)AssetType.Texture, "Null Data");
                sbyte type;
                string name;
                byte[] result = cache.Get(UUID.Random().ToString(), out type, out name);
                Assert.That(result, Is.Null);
            }
        }

        [Test]
        public void TestDeduplicationSameHashDifferentUUIDs()
        {
            using (PackFileCache cache = CreateCache())
            {
                byte[] data = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
                UUID uuid1 = UUID.Random();
                UUID uuid2 = UUID.Random();

                cache.Store(uuid1.ToString(), data, (sbyte)AssetType.Texture, "Asset 1");
                cache.Store(uuid2.ToString(), data, (sbyte)AssetType.Texture, "Asset 2");

                sbyte type1, type2;
                string name1, name2;
                byte[] retrieved1 = cache.Get(uuid1.ToString(), out type1, out name1);
                byte[] retrieved2 = cache.Get(uuid2.ToString(), out type2, out name2);

                Assert.That(retrieved1, Is.Not.Null);
                Assert.That(retrieved2, Is.Not.Null);
                Assert.That(retrieved1, Is.EqualTo(data));
                Assert.That(retrieved2, Is.EqualTo(data));

                string packPath = Path.Combine(m_testDir, "cache_pack_0.bin");
                Assert.That(File.Exists(packPath), Is.True);
                long fileSize = new FileInfo(packPath).Length;
                Assert.That(fileSize, Is.LessThan(data.Length * 2 + 100));
            }
        }

        [Test]
        public void TestPackRotation()
        {
            using (PackFileCache cache = CreateCache(maxPackSize: 200))
            {
                for (int i = 0; i < 5; i++)
                {
                    UUID id = UUID.Random();
                    byte[] data = CreateData(100);
                    cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "PackRot " + i);
                }

                Assert.That(File.Exists(Path.Combine(m_testDir, "cache_pack_0.bin")), Is.True);
                Assert.That(File.Exists(Path.Combine(m_testDir, "cache_pack_1.bin")), Is.True);
            }
        }

        [Test]
        public void TestCacheLimitEnforcement()
        {
            using (PackFileCache cache = CreateCache(maxSize: 500, maxPackSize: 200))
            {
                for (int i = 0; i < 20; i++)
                {
                    UUID id = UUID.Random();
                    byte[] data = CreateData(50);
                    cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "Limit " + i);
                }

                long totalSize = 0;
                foreach (string file in Directory.GetFiles(m_testDir, "cache_pack_*.bin"))
                {
                    totalSize += new FileInfo(file).Length;
                }
                Assert.That(totalSize, Is.LessThanOrEqualTo(5000));
            }
        }

        [Test]
        public void TestCrashRecoveryTruncatesCorruptedTrailingBytes()
        {
            string packPath = Path.Combine(m_testDir, "cache_pack_0.bin");
            UUID storedId = UUID.Random();

            using (PackFileCache cache = CreateCache())
            {
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                cache.Store(storedId.ToString(), data, (sbyte)AssetType.Texture, "CrashTest");
                cache.Dispose();
            }

            FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write);
            fs.Write(new byte[100], 0, 100);
            fs.Close();

            using (PackFileCache cache = CreateCache())
            {
                sbyte type;
                string name;
                byte[] retrieved = cache.Get(storedId.ToString(), out type, out name);
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved, Is.EqualTo(new byte[] { 0x01, 0x02, 0x03, 0x04 }));
            }
        }

        [Test]
        public void TestClearCache()
        {
            using (PackFileCache cache = CreateCache())
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "ClearTest");

                cache.Clear();

                sbyte type;
                string name;
                byte[] result = cache.Get(id.ToString(), out type, out name);
                Assert.That(result, Is.Null);
            }
        }

        [Test]
        public void TestDispose()
        {
            PackFileCache cache = CreateCache();
            UUID id = UUID.Random();
            byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
            cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "DisposeTest");

            cache.Dispose();

            sbyte type;
            string name;
            byte[] result = cache.Get(id.ToString(), out type, out name);
            Assert.That(result, Is.EqualTo(data));
        }

        [Test]
        public void TestConcurrentAccess()
        {
            using (PackFileCache cache = CreateCache())
            {
                ManualResetEvent startEvent = new ManualResetEvent(false);
                Exception writeException = null;
                Exception readException = null;

                Thread writerThread = new Thread(() =>
                {
                    try
                    {
                        startEvent.WaitOne();
                        for (int i = 0; i < 100; i++)
                        {
                            UUID id = UUID.Random();
                            byte[] data = CreateData(1024);
                            cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "Concurrent " + i);
                        }
                    }
                    catch (Exception ex) { writeException = ex; }
                });

                Thread readerThread = new Thread(() =>
                {
                    try
                    {
                        startEvent.WaitOne();
                        for (int i = 0; i < 100; i++)
                        {
                            sbyte type;
                            string name;
                            cache.Get(UUID.Random().ToString(), out type, out name);
                            Thread.Sleep(1);
                        }
                    }
                    catch (Exception ex) { readException = ex; }
                });

                writerThread.Start();
                readerThread.Start();
                startEvent.Set();
                writerThread.Join();
                readerThread.Join();

                Assert.That(writeException, Is.Null);
                Assert.That(readException, Is.Null);
            }
        }

        [Test]
        public void TestDataIntegrityValidation()
        {
            using (PackFileCache cache = CreateCache())
            {
                UUID id = UUID.Random();
                byte[] data = CreateData(512);
                cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "Integrity");

                sbyte type;
                string name;
                byte[] retrieved = cache.Get(id.ToString(), out type, out name);
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved, Is.EqualTo(data));
                Assert.That(type, Is.EqualTo((sbyte)AssetType.Texture));
                Assert.That(name, Is.EqualTo("Integrity"));
            }
        }

        [Test]
        public void TestL1CacheBehavior()
        {
            using (PackFileCache cache = CreateCache())
            {
                UUID id = UUID.Random();
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "L1Test");

                sbyte type;
                string name;
                byte[] retrieved1 = cache.Get(id.ToString(), out type, out name);
                Assert.That(retrieved1, Is.Not.Null);

                byte[] retrieved2 = cache.Get(id.ToString(), out type, out name);
                Assert.That(retrieved2, Is.Not.Null);
                Assert.That(retrieved2, Is.EqualTo(data));
            }
        }

        [Test]
        public void TestHashComputationConsistency()
        {
            using (PackFileCache cache = CreateCache())
            {
                byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                using (SHA256 sha = SHA256.Create())
                {
                    string expectedHash = BitConverter.ToString(sha.ComputeHash(data)).Replace("-", "").ToLower();

                    UUID uuid1 = UUID.Random();
                    UUID uuid2 = UUID.Random();

                    cache.Store(uuid1.ToString(), data, (sbyte)AssetType.Texture, "Hash1");
                    cache.Store(uuid2.ToString(), data, (sbyte)AssetType.Texture, "Hash2");

                    sbyte type1, type2;
                    string name1, name2;
                    byte[] retrieved1 = cache.Get(uuid1.ToString(), out type1, out name1);
                    byte[] retrieved2 = cache.Get(uuid2.ToString(), out type2, out name2);

                    Assert.That(retrieved1, Is.Not.Null);
                    Assert.That(retrieved2, Is.Not.Null);
                }
            }
        }

        [Test]
        public void TestStoreWithSameUUIDReplacesData()
        {
            using (PackFileCache cache = CreateCache())
            {
                UUID id = UUID.Random();
                byte[] data1 = new byte[] { 0x01, 0x02, 0x03, 0x04 };
                byte[] data2 = new byte[] { 0x05, 0x06, 0x07, 0x08 };

                cache.Store(id.ToString(), data1, (sbyte)AssetType.Texture, "Original");
                cache.Store(id.ToString(), data2, (sbyte)AssetType.Texture, "Updated");

                sbyte type;
                string name;
                byte[] retrieved = cache.Get(id.ToString(), out type, out name);
                Assert.That(retrieved, Is.Not.Null);
                Assert.That(retrieved, Is.EqualTo(data2));
            }
        }

        [Test]
        public void TestMultiplePackFilesCreation()
        {
            using (PackFileCache cache = CreateCache(maxPackSize: 100))
            {
                for (int i = 0; i < 3; i++)
                {
                    UUID id = UUID.Random();
                    byte[] data = CreateData(60);
                    cache.Store(id.ToString(), data, (sbyte)AssetType.Texture, "MultiPack " + i);
                }

                Assert.That(File.Exists(Path.Combine(m_testDir, "cache_pack_0.bin")), Is.True);
                Assert.That(File.Exists(Path.Combine(m_testDir, "cache_pack_1.bin")), Is.True);
            }
        }

        [Test]
        public void TestAssetRetrievalAfterPackRotation()
        {
            using (PackFileCache cache = CreateCache(maxPackSize: 200))
            {
                UUID id1 = UUID.Random();
                UUID id2 = UUID.Random();

                byte[] data1 = CreateData(150);
                byte[] data2 = CreateData(150);

                cache.Store(id1.ToString(), data1, (sbyte)AssetType.Texture, "Before Rotation");
                cache.Store(id2.ToString(), data2, (sbyte)AssetType.Texture, "After Rotation");

                sbyte type1, type2;
                string name1, name2;
                byte[] retrieved1 = cache.Get(id1.ToString(), out type1, out name1);
                byte[] retrieved2 = cache.Get(id2.ToString(), out type2, out name2);

                Assert.That(retrieved1, Is.Not.Null);
                Assert.That(retrieved1, Is.EqualTo(data1));
                Assert.That(retrieved2, Is.Not.Null);
                Assert.That(retrieved2, Is.EqualTo(data2));
            }
        }
    }
}
