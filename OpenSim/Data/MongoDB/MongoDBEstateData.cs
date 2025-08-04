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
            // Assuming EstateSettings has a RegionIDs field (List<UUID>) that stores linked region UUIDs
            var filter = Builders<EstateSettings>.Filter.AnyEq(e => e.RegionIDs, regionID);
            EstateSettings es = m_mongodata.Collection.Find(filter).FirstOrDefault();

            if (es == null && create)
            {
                // If not found and create is true, create a new estate
                es = CreateNewEstate(0); // Pass 0 to CreateNewEstate to auto-generate ID
                LinkRegion(regionID, (int)es.EstateID); // Link the region to the new estate
            }

            if (es != null)
            {
                es.OnSave += StoreEstateSettings;
            }

            return es;
        }

        

        public EstateSettings CreateNewEstate(int estateID)
        {
            EstateSettings es = new EstateSettings();
            es.OnSave += StoreEstateSettings;

            // If estateID is 0, generate a new one. Otherwise, use the provided ID.
            if (estateID == 0)
            {
                // Find the maximum existing EstateID and increment it
                var maxEstate = m_mongodata.Collection
                    .Find(Builders<EstateSettings>.Filter.Empty)
                    .SortByDescending(e => e.EstateID)
                    .Limit(1)
                    .FirstOrDefault();

                uint maxId = maxEstate?.EstateID ?? 0;

                if (maxId < 100)
                    maxId = 100;

                es.EstateID = ++maxId;
            }
            else
            {
                es.EstateID = (uint)estateID;
            }

            // Store the new estate settings
            StoreEstateSettings(es);

            return es;
        }


        public void StoreEstateSettings(EstateSettings es)
        {
            try
            {
                var filter = Builders<EstateSettings>.Filter.Eq(e => e.EstateID, es.EstateID);
                var options = new ReplaceOptions { IsUpsert = true };
                m_mongodata.Collection.ReplaceOne(filter, es, options);
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[ESTATE DB]: Error storing estate settings: {0}", e.Message);
            }
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
            try
            {
                var filter = Builders<EstateSettings>.Filter.Eq(e => e.EstateID, (uint)estateID);
                var deleteResult = m_mongodata.Collection.DeleteOne(filter);

                return deleteResult.IsAcknowledged && deleteResult.DeletedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[ESTATE DB]: Error deleting estate: {0}", e.Message);
                return false;
            }
        }
    }
}
