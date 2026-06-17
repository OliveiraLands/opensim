using System;
using System.IO;
using System.Reflection;
using System.Threading;
using log4net;
using Nini.Config;
using Amazon.S3;
using Amazon.S3.Model;
using OpenSim.Framework;

namespace OpenSim.Services.AdvancedAssetService
{
    public class S3BackgroundReplicator : IDisposable
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly string m_serviceUrl;
        private readonly string m_region;
        private readonly string m_bucketName;
        private readonly string m_accessKey;
        private readonly string m_secretKey;
        private readonly string m_storagePath;
        private readonly int m_syncIntervalMs;

        private IAmazonS3 m_s3Client;
        private System.Timers.Timer m_syncTimer;
        private bool m_isSyncing = false;

        public S3BackgroundReplicator(IConfig config, string storagePath)
        {
            m_storagePath = storagePath;

            m_accessKey = config.GetString("S3AccessKey", string.Empty).Trim();
            m_secretKey = config.GetString("S3SecretKey", string.Empty).Trim();
            m_bucketName = config.GetString("S3BucketName", string.Empty).Trim();
            m_serviceUrl = config.GetString("S3ServiceUrl", string.Empty).Trim();
            m_region = config.GetString("S3Region", "us-east-1").Trim();
            
            int intervalMinutes = config.GetInt("S3SyncInterval", 10);
            if (intervalMinutes < 1) intervalMinutes = 1;
            m_syncIntervalMs = intervalMinutes * 60 * 1000;

            // La rotina somente é executada caso esteja configuradas as chaves de acesso ao S3 e o bucket
            if (string.IsNullOrEmpty(m_accessKey) || string.IsNullOrEmpty(m_secretKey) || string.IsNullOrEmpty(m_bucketName))
            {
                m_log.Info("[ADVANCED ASSET SERVICE S3]: S3 replication is disabled (missing S3AccessKey, S3SecretKey or S3BucketName)");
                return;
            }

            try
            {
                AmazonS3Config s3Config = new AmazonS3Config();
                if (!string.IsNullOrEmpty(m_serviceUrl))
                {
                    s3Config.ServiceURL = m_serviceUrl;
                }
                else
                {
                    s3Config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(m_region);
                }
                s3Config.ForcePathStyle = true;

                m_s3Client = new AmazonS3Client(m_accessKey, m_secretKey, s3Config);
                
                m_syncTimer = new System.Timers.Timer(m_syncIntervalMs);
                m_syncTimer.AutoReset = true;
                m_syncTimer.Elapsed += (s, e) => ExecuteSync();
                m_syncTimer.Start();

                m_log.InfoFormat("[ADVANCED ASSET SERVICE S3]: S3 replication enabled for bucket {0} every {1} minutes", m_bucketName, intervalMinutes);
                
                // Run an initial sync in background after 30 seconds
                System.Threading.Tasks.Task.Run(async () => {
                    await System.Threading.Tasks.Task.Delay(30000);
                    ExecuteSync();
                });
            }
            catch (Exception ex)
            {
                m_log.Error("[ADVANCED ASSET SERVICE S3]: Failed to initialize S3 client: " + ex.Message);
            }
        }

        private void ExecuteSync()
        {
            DoSync(msg => m_log.Debug("[ADVANCED ASSET SERVICE S3]: " + msg));
        }

        public void ForceSync(Action<string> logWriter)
        {
            DoSync(logWriter);
        }

        private void DoSync(Action<string> logWriter)
        {
            if (m_isSyncing || m_s3Client == null) return;
            m_isSyncing = true;

            logWriter("Starting S3 synchronization...");

            try
            {
                if (!Directory.Exists(m_storagePath))
                {
                    logWriter(string.Format("Storage path {0} does not exist. Aborting sync.", m_storagePath));
                    return;
                }

                // 1. Sync PackFiles (*.bin)
                string[] files = Directory.GetFiles(m_storagePath, "pack_*.bin");
                int packUploaded = 0;
                foreach (string file in files)
                {
                    try
                    {
                        string filename = Path.GetFileName(file);
                        string s3Key = "packfiles/" + filename;
                        FileInfo fi = new FileInfo(file);
                        long localLength = fi.Length;

                        // Check if file already exists in S3 with same size
                        if (!FileExistsInS3(s3Key, localLength))
                        {
                            logWriter(string.Format("Uploading {0} ({1} bytes) to S3...", filename, localLength));
                            UploadFile(file, s3Key);
                            packUploaded++;
                            logWriter(string.Format("Upload completed: {0}", filename));
                        }
                    }
                    catch (Exception fileEx)
                    {
                        logWriter("Error syncing file " + file + ": " + fileEx.Message);
                    }
                }

                // 2. Sync SQLite Index DB (index.db)
                string indexFile = Path.Combine(m_storagePath, "index.db");
                if (File.Exists(indexFile))
                {
                    string tempIndex = Path.Combine(m_storagePath, "index_s3_upload.db");
                    try
                    {
                        File.Copy(indexFile, tempIndex, true);
                        logWriter("Uploading SQLite index snapshot to S3...");
                        UploadFile(tempIndex, "metadata/index.db");
                        logWriter("SQLite index snapshot upload completed.");
                    }
                    catch (Exception dbEx)
                    {
                        logWriter("Error syncing SQLite index database: " + dbEx.Message);
                    }
                    finally
                    {
                        try
                        {
                            if (File.Exists(tempIndex))
                                File.Delete(tempIndex);
                        }
                        catch {}
                    }
                }
                logWriter("S3 synchronization finished.");
            }
            catch (Exception ex)
            {
                logWriter("Global error during S3 sync: " + ex.Message);
            }
            finally
            {
                m_isSyncing = false;
            }
        }

        private void UploadFile(string localPath, string s3Key)
        {
            PutObjectRequest request = new PutObjectRequest
            {
                BucketName = m_bucketName,
                Key = s3Key,
                FilePath = localPath
            };
            m_s3Client.PutObjectAsync(request).Wait();
        }

        private bool FileExistsInS3(string s3Key, long expectedSize)
        {
            try
            {
                var response = m_s3Client.GetObjectMetadataAsync(m_bucketName, s3Key).Result;
                return response.ContentLength == expectedSize;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            m_syncTimer?.Stop();
            m_s3Client?.Dispose();
        }
    }
}
