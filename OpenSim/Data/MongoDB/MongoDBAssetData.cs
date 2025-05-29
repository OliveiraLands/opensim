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

using log4net;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using OpenMetaverse;
using OpenSim.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace OpenSim.Data.MongoDB
{
    /// <summary>
    /// An asset storage interface for the MongoDB database system
    /// </summary>
    public class MongoDBAssetData : AssetDataBase
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected MongoDBGenericTableHandler<AssetBase> m_assetbase;
        // protected static MongoClient m_Connection;
        // protected static IMongoDatabase m_mongoDatabase;
        private   IMongoCollection<AssetBase> _collection;

        private const string storeName = "Assets";

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        override public void Dispose()
        {
        }

        /// <summary>
        /// <list type="bullet">
        /// <item>Initialises AssetData interface</item>
        /// <item>Loads and initialises a new MongoDB connection and maintains it.</item>
        /// <item>use default URI if connect string is empty.</item>
        /// </list>
        /// </summary>
        /// <param name="dbconnect">connect string</param>
        override public void Initialise(string dbconnect)
        {
            m_assetbase = new MongoDBGenericTableHandler<AssetBase>(dbconnect, storeName, storeName, "FullID");

            /*
            var mongoUrl = new MongoUrl(dbconnect);
            m_Connection = new MongoClient(dbconnect);

            m_mongoDatabase = m_Connection.GetDatabase(mongoUrl.DatabaseName);

            _collection = m_mongoDatabase.GetCollection<AssetBase>(storeName);

            if (!BsonClassMap.IsClassMapRegistered(typeof(AssetBase)))
            {
                BsonClassMap.RegisterClassMap<AssetDataBase>(cm =>
                {
                    cm.AutoMap();
                    cm.SetIgnoreExtraElements(true); // Ignora campos extras no documento BSON

                    // Procura o campo especificado por nome
                    var propInfo = typeof(AssetBase).GetProperty("ID", BindingFlags.Public | BindingFlags.Instance);

                    if (propInfo == null)
                        throw new InvalidOperationException($"A propriedade 'ID' não existe na classe '{typeof(AssetBase).Name}'.");

                    // Mapeia a propriedade como Id
                    cm.MapIdMember(propInfo)
                      .SetIdGenerator(StringObjectIdGenerator.Instance); // Pode mudar o gerador conforme o tipo do campo
                });
            }
            */
            return;
        }

        /// <summary>
        /// Fetch Asset
        /// </summary>
        /// <param name="uuid">UUID of ... ?</param>
        /// <returns>Asset base</returns>
        override public AssetBase GetAsset(UUID uuid)
        {
            lock (this)
            {
               AssetBase m_return = m_assetbase.Get("FullID", uuid.ToString()).FirstOrDefault();

                // If not found, try to fetch from MongoDB
                /*AssetBase retorno = _collection.Find(x => x.FullID == uuid)
                    .FirstOrDefaultAsync()
                    .Result;*/

                return m_return ?? null;
            }
        }

        /// <summary>
        /// Create an asset
        /// </summary>
        /// <param name="asset">Asset Base</param>
        override public bool StoreAsset(AssetBase asset)
        {
            string assetName = asset.Name;
            if (asset.Name.Length > AssetBase.MAX_ASSET_NAME)
            {
                assetName = asset.Name.Substring(0, AssetBase.MAX_ASSET_NAME);
                m_log.WarnFormat(
                    "[ASSET DB]: Name '{0}' for asset {1} truncated from {2} to {3} characters on add",
                    asset.Name, asset.ID, asset.Name.Length, assetName.Length);
            }

            string assetDescription = asset.Description;
            if (asset.Description.Length > AssetBase.MAX_ASSET_DESC)
            {
                assetDescription = asset.Description.Substring(0, AssetBase.MAX_ASSET_DESC);
                m_log.WarnFormat(
                    "[ASSET DB]: Description '{0}' for asset {1} truncated from {2} to {3} characters on add",
                    asset.Description, asset.ID, asset.Description.Length, assetDescription.Length);
            }

            return m_assetbase.Store(asset);
            /*
            var filter = Builders<AssetBase>.Filter.Eq(a => a.FullID, asset.FullID);
            var options = new ReplaceOptions { IsUpsert = true };

            // Atualizar registro existente (ou inserir se IsUpsert for true)
            var result = _collection.ReplaceOneAsync(filter, asset, options).Result;
            return result.IsAcknowledged && (result.ModifiedCount > 0 || result.UpsertedId != null);
            */
        }

        //        /// <summary>
        //        /// Some... logging functionnality
        //        /// </summary>
        //        /// <param name="asset"></param>
        //        private static void LogAssetLoad(AssetBase asset)
        //        {
        //            string temporary = asset.Temporary ? "Temporary" : "Stored";
        //            string local = asset.Local ? "Local" : "Remote";
        //
        //            int assetLength = (asset.Data != null) ? asset.Data.Length : 0;
        //
        //            m_log.Debug("[ASSET DB]: " +
        //                                     string.Format("Loaded {5} {4} Asset: [{0}][{3}] \"{1}\":{2} ({6} bytes)",
        //                                                   asset.FullID, asset.Name, asset.Description, asset.Type,
        //                                                   temporary, local, assetLength));
        //        }

        /// <summary>
        /// Check if the assets exist in the database.
        /// </summary>
        /// <param name="uuids">The assets' IDs</param>
        /// <returns>For each asset: true if it exists, false otherwise</returns>
        public override bool[] AssetsExist(UUID[] uuids)
        {
            if (uuids.Length == 0)
                return Array.Empty<bool>();

            // Converte os UUIDs para strings (ajuste se for GUID ou outro tipo no MongoDB)
            var stringUuids = uuids.Select(u => u.ToString()).ToArray();

            // Chama a rotina genérica
            return m_assetbase.ItemsExist<AssetBase>(stringUuids, a => a.FullID, _collection);

            /*
            if (uuids.Length == 0)
                return new bool[0];

            // Converte os UUIDs para a representação de string ou Guid esperada no MongoDB
            var stringUuids = uuids.Select(u => u.ToString()).ToList(); // Ajuste aqui conforme o tipo real de UUID

            // Cria um filtro para buscar documentos onde o campo 'Id' (mapeado de UUID) está na lista de stringUuids
            var filter = Builders<AssetBase>.Filter.In(a => a.FullID.ToString(), stringUuids);

            // Projeta apenas o campo Id para reduzir a quantidade de dados transferidos
            var projection = Builders<AssetBase>.Projection.Include(a => a.FullID);

            // Busca no MongoDB e obtém os IDs existentes
            var existingAssetsIds = _collection
                .Find(filter)
                .Project(projection)
                .ToList();

            // Converte a lista de BsonDocuments para HashSet de UUIDs para busca eficiente
            // Se o Id é um ObjectId em BsonType.ObjectId, você precisará de conversão adicional.
            // Se é string ou Guid, a conversão é mais direta.
            var existingUuidsInDb = new HashSet<string>(existingAssetsIds.Select(doc => doc["_id"].AsString)); // Ajustar conforme o tipo de _id no DB

            bool[] results = new bool[uuids.Count()];
            int i = 0;
            foreach (var uuid in uuids)
            {
                results[i] = existingUuidsInDb.Contains(uuid.ToString()); // Compara a string do UUID
                i++;
            }

            return results;
            */
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private static AssetBase buildAsset(IDataReader row)
        {
            // TODO: this doesn't work yet because something more
            // interesting has to be done to actually get these values
            // back out.  Not enough time to figure it out yet.
            AssetBase asset = new AssetBase(
                new UUID((String)row["UUID"]),
                (String)row["Name"],
                Convert.ToSByte(row["Type"]),
                (String)row["CreatorID"]
            );

            asset.Description = (String) row["Description"];
            asset.Local = Convert.ToBoolean(row["Local"]);
            asset.Temporary = Convert.ToBoolean(row["Temporary"]);
            asset.Flags = (AssetFlags)Convert.ToInt32(row["asset_flags"]);
            asset.Data = (byte[])row["Data"];
            asset.Hash = (row["hash"] == DBNull.Value ? "" : (string)row["hash"]);
            return asset;
        }

        private static AssetMetadata buildAssetMetadata(IDataReader row)
        {
            AssetMetadata metadata = new AssetMetadata();

            metadata.FullID = new UUID((string) row["UUID"]);
            metadata.Name = (string) row["Name"];
            metadata.Description = (string) row["Description"];
            metadata.Type = Convert.ToSByte(row["Type"]);
            metadata.Temporary = Convert.ToBoolean(row["Temporary"]); // Not sure if this is correct.
            metadata.Flags = (AssetFlags)Convert.ToInt32(row["asset_flags"]);
            metadata.CreatorID = row["CreatorID"].ToString();
            metadata.Hash = (row["hash"] == DBNull.Value ? "" : (string)row["hash"]);

            // Current SHA1s are not stored/computed.
            metadata.SHA1 = Array.Empty<byte>();

            return metadata;
        }

        /// <summary>
        /// Returns a list of AssetMetadata objects. The list is a subset of
        /// the entire data set offset by <paramref name="start" /> containing
        /// <paramref name="count" /> elements.
        /// </summary>
        /// <param name="start">The number of results to discard from the total data set.</param>
        /// <param name="count">The number of rows the returned list should contain.</param>
        /// <returns>A list of AssetMetadata objects.</returns>
        public override List<AssetMetadata> FetchAssetMetadataSet(int start, int count)
        {
            List<AssetMetadata> retList = new List<AssetMetadata>(count);

            lock (this)
            {
                if (start < 0) start = 0;
                if (count < 1) return new List<AssetMetadata>(); // Garante que count seja positivo

                // Define um filtro vazio para retornar todos os documentos
                var filter = Builders<AssetBase>.Filter.Empty;

                // Cria a lista de metadados de forma assíncrona
               retList = _collection.Find(filter)
                                             .Skip(start) // Pula os primeiros 'start' documentos
                                             .Limit(count) // Limita o resultado aos próximos 'count' documentos
                                             .Project(assetBase => new AssetMetadata
                                             {
                                                 FullID = assetBase.FullID,
                                                 Name = assetBase.Name,
                                                 Description = assetBase.Description,
                                                 Type = assetBase.Type,
                                                 Temporary = assetBase.Temporary,
                                                 Flags = assetBase.Flags,
                                                 CreatorID = assetBase.CreatorID,
                                                 Hash = assetBase.Hash
                                             })
                                             .ToListAsync()
                                             .GetAwaiter().GetResult(); // Executa a consulta e retorna a lista

            }

            return retList;
        }

        /***********************************************************************
         *
         *  Database Binding functions
         *
         *  These will be db specific due to typing, and minor differences
         *  in databases.
         *
         **********************************************************************/

        #region IPlugin interface

        /// <summary>
        ///
        /// </summary>
        override public string Version
        {
            get
            {
                Module module = GetType().Module;
                // string dllName = module.Assembly.ManifestModule.Name;
                Version dllVersion = module.Assembly.GetName().Version;

                return
                    string.Format("{0}.{1}.{2}.{3}", dllVersion.Major, dllVersion.Minor, dllVersion.Build,
                                  dllVersion.Revision);
            }
        }

        /// <summary>
        /// Initialise the AssetData interface using default URI
        /// </summary>
        override public void Initialise()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Name of this DB provider
        /// </summary>
        override public string Name
        {
            get { return "MongoDB Asset storage engine"; }
        }

        // TODO: (AlexRa): one of these is to be removed eventually (?)

        /// <summary>
        /// Delete an asset from database
        /// </summary>
        /// <param name="uuid"></param>
        public bool DeleteAsset(UUID uuid)
        {
            lock (this)
            {
                return m_assetbase.Delete("FullID", uuid.ToString());
                /*
                var filter = Builders<AssetBase>.Filter.Eq(a => a.FullID.ToString(), uuid.ToString());

                // Executa a operação de exclusão de um único documento
                var result = _collection.DeleteOneAsync(filter).Result;

                // Retorna true se um documento foi excluído (DeletedCount > 0)
                return result.DeletedCount > 0;
                */
            }
        }

        public override bool Delete(string id)
        {
            UUID assetID;

            if (!UUID.TryParse(id, out assetID))
                return false;

            return DeleteAsset(assetID);
        }

        #endregion
    }
}
