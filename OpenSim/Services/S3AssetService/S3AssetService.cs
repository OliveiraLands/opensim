/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using log4net;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Data;
using OpenSim.Framework;
using OpenSim.Framework.Serialization.External;
using OpenSim.Services.Base;
using OpenSim.Services.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading;


namespace OpenSim.Services.S3AssetService
{
    public class S3AssetConnector : ServiceBase, IAssetService
    {
        private readonly string cacheDirectory;               // Diretório de cache local
        private AmazonS3Client m_S3Client;
        private string m_S3BucketName;

        public S3AssetConnector(IConfigSource config, string configName) : base(config)
        {
            // Configurações do S3
            IConfig s3Config = config.Configs["S3"];
            if (s3Config == null)
                throw new Exception("No S3 configuration");

            string accessKey = s3Config.GetString("AccessKey", "");
            string secretKey = s3Config.GetString("SecretKey", "");
            m_S3BucketName = s3Config.GetString("BucketName", "");
            string region = s3Config.GetString("Region", "us-east-1");

            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            m_S3Client = new AmazonS3Client(credentials, RegionEndpoint.GetBySystemName(region));


        }

        // Método auxiliar para calcular hash SHA-256
        private string GetSHA256Hash(byte[] data)
        {
            using (var sha256 = SHA256.Create())
            {
                byte[] hash = sha256.ComputeHash(data);
                return BitConverter.ToString(hash).Replace("-", "").ToLower();
            }
        }

        // Método auxiliar para comprimir dados
        private byte[] CompressData(byte[] data)
        {
            using (var ms = new MemoryStream())
            using (var gzip = new GZipStream(ms, CompressionMode.Compress))
            {
                gzip.Write(data, 0, data.Length);
                gzip.Close();
                return ms.ToArray();
            }
        }

        // Método auxiliar para descomprimir dados
        private byte[] DecompressData(byte[] compressedData)
        {
            using (var ms = new MemoryStream(compressedData))
            using (var gzip = new GZipStream(ms, CompressionMode.Decompress))
            using (var output = new MemoryStream())
            {
                gzip.CopyTo(output);
                return output.ToArray();
            }
        }

        // Converter hash em caminho (ex.: "012345" -> "01/23/45")
        private string HashToPath(string hash)
        {
            return $"{hash.Substring(0, 2)}/{hash.Substring(2, 2)}/{hash.Substring(4, 2)}";
        }

        // Armazenar ativo
        public string Store(AssetBase asset)
        {
            string hash = GetSHA256Hash(asset.Data);
            byte[] compressedData = CompressData(asset.Data);
            string s3Key = $"assets/{HashToPath(hash)}/{hash}.gz";
            string localPath = Path.Combine(cacheDirectory, HashToPath(hash), $"{hash}.gz");

            // Salvar no S3
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = s3Key,
                InputStream = new MemoryStream(compressedData),
                ContentType = "application/gzip"
            };
            s3Client.PutObjectAsync(putRequest).Wait(); // Usar await em código assíncrono

            // Salvar no cache local
            Directory.CreateDirectory(Path.GetDirectoryName(localPath));
            File.WriteAllBytes(localPath, compressedData);

            // Atualizar banco de dados
            m_DataConnector.Store(asset.Metadata, hash);

            return asset.ID;
        }

        // Recuperar ativo
        public AssetBase Get(string id)
        {
            string hash;
            AssetMetadata metadata = m_DataConnector.Get(id, out hash);
            if (metadata == null) return null;

            string localPath = Path.Combine(cacheDirectory, HashToPath(hash), $"{hash}.gz");
            string s3Key = $"assets/{HashToPath(hash)}/{hash}.gz";
            byte[] data;

            if (File.Exists(localPath))
            {
                // Ler do cache local
                data = DecompressData(File.ReadAllBytes(localPath));
            }
            else
            {
                // Buscar no S3
                var getRequest = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = s3Key
                };
                using (var response = s3Client.GetObjectAsync(getRequest).Result) // Usar await em código assíncrono
                using (var ms = new MemoryStream())
                {
                    response.ResponseStream.CopyTo(ms);
                    byte[] compressedData = ms.ToArray();
                    File.WriteAllBytes(localPath, compressedData); // Atualizar cache
                    data = DecompressData(compressedData);
                }
            }

            return new AssetBase { Metadata = metadata, Data = data };
        }

        // Excluir ativo
        public bool Delete(string id)
        {
            string hash;
            AssetMetadata metadata = m_DataConnector.Get(id, out hash);
            if (metadata == null) return false;

            // Excluir do banco de dados
            m_DataConnector.Delete(id);

            // Excluir do cache local
            string localPath = Path.Combine(cacheDirectory, HashToPath(hash), $"{hash}.gz");
            if (File.Exists(localPath)) File.Delete(localPath);

            // Excluir do S3
            string s3Key = $"assets/{HashToPath(hash)}/{hash}.gz";
            var deleteRequest = new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = s3Key
            };
            s3Client.DeleteObjectAsync(deleteRequest).Wait(); // Usar await em código assíncrono

            return true;
        }
    }
}