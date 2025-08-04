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
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBGenericTableHandler<T> : MongoDBFramework where T : class, new()
    {
        //        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected string m_Realm;

        protected static MongoClient m_Connection;
        protected static IMongoDatabase m_mongoDatabase;
        private readonly IMongoCollection<T> _collection;

        public IMongoCollection<T> Collection
        {
            get { return _collection; }
        }

        private static bool m_initialized;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MongoDBGenericTableHandler(string connectionString,
                string realm, string storeName, string campoId) : base(connectionString)
        {
            m_Realm = realm;

            if (!m_initialized)
            {
                var mongoUrl = new MongoUrl(connectionString);

                m_Connection = new MongoClient(connectionString);
                //Console.WriteLine(string.Format("OPENING CONNECTION FOR {0} USING {1}", storeName, connectionString));

                m_mongoDatabase = m_Connection.GetDatabase(mongoUrl.DatabaseName);

                _collection = m_mongoDatabase.GetCollection<T>(storeName);

                RegistrarMapeamentoClasse<T>(campoId);

                if (storeName != String.Empty)
                {
                    // Build instructions for MondoDB data migration
                    // Migration m = new Migration(m_Connection, Assembly, storeName);
                }

                m_initialized = true;
            }

        }

        public static void RegistrarMapeamentoClasse<T>(string nomeCampoId)
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof(T)))
            {
                BsonClassMap.RegisterClassMap<T>(cm =>
                {
                    cm.AutoMap();
                    cm.SetIgnoreExtraElements(true); // Ignora campos extras no documento BSON

                    // Procura o campo especificado por nome
                    var propInfo = typeof(T).GetProperty(nomeCampoId, BindingFlags.Public | BindingFlags.Instance);

                    if (propInfo == null)
                        throw new InvalidOperationException($"A propriedade '{nomeCampoId}' n�o existe na classe '{typeof(T).Name}'.");

                    // Mapeia a propriedade como Id
                    cm.MapIdMember(propInfo)
                      .SetIdGenerator(StringObjectIdGenerator.Instance); // Pode mudar o gerador conforme o tipo do campo
                });
            }
        }

        public virtual T[] Get(string field, string key)
        {
            return Get(new string[] { field }, new string[] { key });
        }

        public virtual T[] Get(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length)
                return new T[0];

            var builder = Builders<T>.Filter;
            var filters = new List<FilterDefinition<T>>();

            for (int i = 0; i < fields.Length; i++)
            {
                filters.Add(builder.Eq(fields[i], keys[i]));
            }

            var combinedFilter = builder.And(filters);

            return _collection.Find(combinedFilter).ToList().ToArray();
        }

        

        public virtual bool Store(T row)
        {
            // Obt�m o tipo da classe T
            var type = typeof(T);

            // Procura a propriedade marcada com [BsonId]
            var idProperty = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                 .FirstOrDefault(prop => Attribute.IsDefined(prop, typeof(BsonIdAttribute)));

            if (idProperty == null)
            {
                throw new InvalidOperationException("A classe de dados deve ter uma propriedade marcada com [BsonId].");
            }

            // Obt�m o valor do _id
            var idValue = idProperty.GetValue(row);
            if (idValue == null)
            {
                throw new InvalidOperationException("O valor do campo [BsonId] n�o pode ser nulo.");
            }

            // Cria o filtro para localizar o documento com o mesmo _id
            var filter = Builders<T>.Filter.Eq(idProperty.Name, idValue);

            // Define as op��es de substitui��o com upsert
            var options = new ReplaceOptions { IsUpsert = true };

            // Executa a substitui��o ou inser��o
            var result = _collection.ReplaceOne(filter, row, options);

            return result.IsAcknowledged && (result.ModifiedCount > 0 || result.UpsertedId != null);
        }

        public virtual bool Delete(string field, string key)
        {
            return Delete(new string[] { field }, new string[] { key });
        }

        public virtual bool Delete(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length)
                return false;

            var builder = Builders<T>.Filter;
            var filters = new List<FilterDefinition<T>>();

            for (int i = 0; i < fields.Length; i++)
            {
                filters.Add(builder.Eq(fields[i], keys[i]));
            }

            var combinedFilter = builder.And(filters);

            var result = _collection.DeleteMany(combinedFilter);

            return result.DeletedCount > 0;
        }

        public bool[] ItemsExist<T>(IEnumerable<string> ids, Expression<Func<T, object>> idField, IMongoCollection<T> collection)
        {
            if (ids == null || !ids.Any())
                return Array.Empty<bool>();

            // Cria filtro para verificar se o campo de ID est� contido na lista
            var filter = Builders<T>.Filter.In(idField, ids);

            // Projeta apenas o campo de ID
            var projection = Builders<T>.Projection.Include(idField);

            // Executa a consulta no MongoDB
            var existingDocs = collection
                .Find(filter)
                .Project(projection)
                .ToList();

            // Extrai os valores do campo de ID retornados
            var existingIds = new HashSet<string>();
            foreach (var doc in existingDocs)
            {
                if (doc.Contains("_id"))
                    existingIds.Add(doc["_id"].ToString());
                else
                    existingIds.Add(doc.GetValue("FullID", "").ToString());
            }

            // Verifica quais IDs existem
            return ids.Select(id => existingIds.Contains(id)).ToArray();
        }
        public bool ItemExists<T>(string id, Expression<Func<T, object>> idField, IMongoCollection<T> collection)
        {
            // Reutiliza a rotina para m�ltiplos, passando apenas um valor
            var resultArray = ItemsExist<T>(new[] { id }, idField, collection);

            // Retorna o primeiro (e �nico) resultado
            return resultArray.Length > 0 && resultArray[0];
        }

    }
}
