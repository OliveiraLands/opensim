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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using OpenMetaverse;
using OpenSim.Framework;
#if CSharpMongoDB
    using Community.CsharpMongoDB.MongoDB;
#else
    using Mono.Data.MongoDB;
#endif

namespace OpenSim.Data.MongoDB
{
    public class MongoDBUserAccountData : MongoDBGenericTableHandler<UserAccountData>, IUserAccountData
    {
        public MongoDBUserAccountData(string connectionString, string realm)
                : base(connectionString, realm, "UserAccount")
        {
        }

        public UserAccountData[] GetUsers(UUID scopeID, string query)
        {
            string[] words = query.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (words.Length == 0 || words.Length > 2)
                return new UserAccountData[0];

            var filters = new List<FilterDefinition<UserAccountData>>();

            // ScopeID filter
            filters.Add(Builders<UserAccountData>.Filter.Or(
                Builders<UserAccountData>.Filter.Eq(u => u.ScopeID, scopeID),
                Builders<UserAccountData>.Filter.Eq(u => u.ScopeID, UUID.Zero)
            ));

            // Name filters
            if (words.Length == 1)
            {
                filters.Add(Builders<UserAccountData>.Filter.Or(
                    Builders<UserAccountData>.Filter.Regex(u => u.FirstName, new BsonRegularExpression("^" + words[0], "i")),
                    Builders<UserAccountData>.Filter.Regex(u => u.LastName, new BsonRegularExpression("^" + words[0], "i"))
                ));
            }
            else // words.Length == 2
            {
                filters.Add(Builders<UserAccountData>.Filter.Or(
                    Builders<UserAccountData>.Filter.Regex(u => u.FirstName, new BsonRegularExpression("^" + words[0], "i")),
                    Builders<UserAccountData>.Filter.Regex(u => u.LastName, new BsonRegularExpression("^" + words[1], "i"))
                ));
            }

            var combinedFilter = Builders<UserAccountData>.Filter.And(filters);

            return Collection.Find(combinedFilter).ToList().ToArray();
        }

        public UserAccountData[] GetUsersWhere(UUID scopeID, string where)
        {
            // This method is intended for more complex, dynamic queries.
            // Directly translating an arbitrary 'where' string to a MongoDB filter
            // can be complex and potentially insecure. For now, it returns an empty array.
            // If full functionality is required, a specific query parsing mechanism
            // or a more structured input would be needed.
            return new UserAccountData[0];
        }
    }
}
