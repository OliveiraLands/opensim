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
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using log4net;
using MongoDB.Driver;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBEstateStore : IEstateDataStore
    {
        private static readonly ILog m_log =
            LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected MongoDBGenericTableHandler<EstateSettings> m_mongodata;
        private string m_connectionString;


        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MongoDBEstateStore()
        {
        }

        public MongoDBEstateStore(string connectionString)
        {
            Initialise(connectionString);
        }

        public void Initialise(string connectionString)
        {
            m_connectionString = connectionString;

            m_log.Info("[ESTATE DB]: MongoDB - connecting: "+m_connectionString);

            m_mongodata = new MongoDBGenericTableHandler<EstateSettings>(m_connectionString, "EstateStore", "EstateStore","EstateID");

        }

        public EstateSettings LoadEstateSettings(UUID regionID, bool create)
        {
            return m_mongodata.Get(regionID.ToString(), "EstateID").First();
        }

        /*
        private EstateSettings DoLoad(MongoDBCommand cmd, UUID regionID, bool create)
        {
            EstateSettings es = new EstateSettings();
            es.OnSave += StoreEstateSettings;
            IDataReader r = null;
            try
            {
                 r = cmd.ExecuteReader();
            }
            catch (MongoDBException)
            {
                m_log.Error("[MongoDB]: There was an issue loading the estate settings.  This can happen the first time running OpenSimulator with CSharpMongoDB the first time.  OpenSimulator will probably crash, restart it and it should be good to go.");
            }

            if (r != null && r.Read())
            {
                foreach (string name in FieldList)
                {
                    if (m_FieldMap[name].GetValue(es) is bool)
                    {
                        int v = Convert.ToInt32(r[name]);
                        if (v != 0)
                            m_FieldMap[name].SetValue(es, true);
                        else
                            m_FieldMap[name].SetValue(es, false);
                    }
                    else if (m_FieldMap[name].GetValue(es) is UUID)
                    {
                        UUID uuid = UUID.Zero;

                        UUID.TryParse(r[name].ToString(), out uuid);
                        m_FieldMap[name].SetValue(es, uuid);
                    }
                    else
                    {
                        m_FieldMap[name].SetValue(es, Convert.ChangeType(r[name], m_FieldMap[name].FieldType));
                    }
                }
                r.Close();
            }
            else if (create)
            {
                DoCreate(es);
                LinkRegion(regionID, (int)es.EstateID);
            }

            LoadBanList(es);

            es.EstateManagers = LoadUUIDList(es.EstateID, "estate_managers");
            es.EstateAccess = LoadUUIDList(es.EstateID, "estate_users");
            es.EstateGroups = LoadUUIDList(es.EstateID, "estate_groups");
            return es;
        }
        */

        public EstateSettings CreateNewEstate(int estateID)
        {
            EstateSettings es = new EstateSettings();
            
            es.OnSave += StoreEstateSettings;
            es.EstateID = Convert.ToUInt32(estateID);

            var maxEstate = m_mongodata.Collection
                .Find(Builders<EstateSettings>.Filter.Empty)
                .SortByDescending(e => e.EstateID)
                .Limit(1)
                .FirstOrDefault();

            uint maxId = maxEstate?.EstateID ?? 0;

            if (maxId < 100)
                maxId = 100;

            es.EstateID = ++maxId;
            es.Save();

            return es;
        }


        public void StoreEstateSettings(EstateSettings es)
        {
            es.Save();
        }


        public EstateSettings LoadEstateSettings(int estateID)
        {
            return m_mongodata.Get("estateID", estateID.ToString()).FirstOrDefault() ??
                   CreateNewEstate(estateID);
        }

        public List<EstateSettings> LoadEstateSettingsAll()
        {
            List<EstateSettings> estateSettings = new List<EstateSettings>();

            List<int> estateIds = GetEstatesAll();
            foreach (int estateId in estateIds)
                estateSettings.Add(LoadEstateSettings(estateId));

            return estateSettings;
        }

        public List<int> GetEstates(string search)
        {
            List<int> result = new List<int>();

            if (string.IsNullOrEmpty(search))
            {
                return GetEstatesAll();
            }
            search = search.Trim().ToLowerInvariant();
            if (search.Length == 0)
            {
                return GetEstatesAll();
            }
            if (search.Length > 64)
            {
                search = search.Substring(0, 64);
            }
            // Search for estates by name
            // Note: This is a simple search, it does not support wildcards or partial matches.
            // If you need more complex searching, consider using a full-text search index or similar.
            // This query assumes that the estate_settings table has a column named EstateName.

            result  = m_mongodata.Collection
                .Find(e => e.EstateName.ToLowerInvariant().Contains(search))
                .Project(e => (int)e.EstateID)
                .ToList();

            return result;
        }

        public List<int> GetEstatesAll()
        {
            List<int> result = m_mongodata.Collection
                .Find(FilterDefinition<EstateSettings>.Empty)
                .Project(e => (int)e.EstateID)
                .ToList();

            return result;
        }

        public List<int> GetEstatesByOwner(UUID ownerID)
        {
            List<int> result = m_mongodata.Collection
                .Find(e => e.EstateOwner == ownerID)             // Filtra pelo campo EstateOwner
                .Project(e => (int)e.EstateID)                   // Projeta apenas EstateID convertido para int
                .ToList();

            return result;
        }

        public bool LinkRegion(UUID regionID, int estateID)
        {
            EstateSettings estate = LoadEstateSettings(estateID);
            if (estate == null)
            {
                m_log.ErrorFormat("[MongoDBEstateStore]: Unable to link region {0} to estate {1} because the estate does not exist.", regionID, estateID);
                return false;
            }
            // Assuming that the regionID is stored in the EstateSettings object
            // You may need to adjust this based on your actual data structure
            if (estate.RegionIDs == null)
            {
                estate.RegionIDs = new List<UUID>();
            }
            if (estate.RegionIDs.Contains(regionID))
            {
                m_log.WarnFormat("[MongoDBEstateStore]: Region {0} is already linked to estate {1}.", regionID, estateID);
                return false;
            }
            estate.RegionIDs.Add(regionID);
            StoreEstateSettings(estate);
            return true;
        }

        public List<UUID> GetRegions(int estateID)
        {
            EstateSettings estate = LoadEstateSettings(estateID);
            if (estate == null)
            {
                m_log.ErrorFormat("[MongoDBEstateStore]: Unable to get estate {0} because the estate does not exist.", estateID);
                return new List<UUID>();
            }
            return estate.RegionIDs ?? new List<UUID>();
        }

        public bool DeleteEstate(int estateID)
        {
            return false;
        }
    }
}
