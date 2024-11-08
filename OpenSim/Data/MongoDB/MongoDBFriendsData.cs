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
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ''AS IS'' AND ANY
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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using OpenMetaverse;
using OpenSim.Framework;
using System.Reflection;
using System.Text;
using MongoDB.Driver;
using MongoDB.Bson;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBFriendsData : MongoDBGenericTableHandler<FriendsData>, IFriendsData
    {
        private MongoClient _mongoClient;
        private MongoDBManager _Database;
        private IMongoDatabase _db;

        public MongoDBFriendsData(string connectionString, string realm)
            : base(connectionString, realm, "FriendsStore")
        {
            /*
            using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
            {
                conn.Open();
                Migration m = new Migration(conn, GetType().Assembly, "FriendsStore");
                m.Update();
            }
            */
            _Database = new MongoDBManager(connectionString);
            _mongoClient = new MongoClient(m_connectionString);
            _db = _mongoClient.GetDatabase(_Database.GetDatabaseName());

        }


        public override bool Delete(string principalID, string friend)
        {
            UUID princUUID = UUID.Zero;

            bool ret = UUID.TryParse(principalID, out princUUID);

            if (ret)
                return Delete(princUUID, friend);
            else
                return false;
        }

        public bool Delete(UUID principalID, string friend)
        {
            var collection = _db.GetCollection<BsonDocument>(m_Realm);

            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("PrincipalID", principalID.ToString()),
                Builders<BsonDocument>.Filter.Eq("Friend", friend)
            );

            var result = collection.DeleteOne(filter);

            return result.DeletedCount > 0; //
        }

        public FriendsData[] GetFriends(string principalID)
        {
            UUID princUUID = UUID.Zero;

            bool ret = UUID.TryParse(principalID, out princUUID);

            if (ret)
               return GetFriends(princUUID);
            else
                return new FriendsData[0];
        }

        public FriendsData[] GetFriends(UUID principalID)
        {
            var collection = _db.GetCollection<BsonDocument>(m_Realm);

            var filter = Builders<BsonDocument>.Filter.Eq("PrincipalID", principalID.ToString());
            var projection = Builders<BsonDocument>.Projection.Include("Friend").Include("Flags");

            var friendsList = collection.Find(filter).Project(projection).ToList();

            var result = new List<FriendsData>();

            foreach (var doc in friendsList)
            {
                string? theirflags = doc["Flags"] != null ? doc["Flags"].AsString : "-1";

                var friendData = new FriendsData
                {
                    // Preencha as propriedades de FriendsData conforme necess�rio, por exemplo:
                    Friend = doc["Friend"].AsString
                    //Data["Friend, "Flags"] = theirflags
                };

                result.Add(friendData);
            }

            return result.ToArray();
        }

        public FriendsData[] GetFriends(Guid principalID)
        {
            return GetFriends(principalID);
        }

    }
}
