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
using System.Reflection;
using log4net;
#if CSharpMongoDB
using Community.CsharpMongoDB.MongoDB;
#else
using Mono.Data.MongoDB;
#endif
using OpenMetaverse;
using OpenMetaverse.StructuredData;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBUserProfilesData: IProfilesData
    {
        private static readonly ILog m_log =
            LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private MongoDBConnection m_connection;
        private string m_connectionString;

        private Dictionary<string, FieldInfo> m_FieldMap =
            new Dictionary<string, FieldInfo>();

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MongoDBUserProfilesData()
        {
        }

        public MongoDBUserProfilesData(string connectionString)
        {
            Initialise(connectionString);
        }

        private IMongoDatabase m_mongoDatabase;
        private MongoClient m_mongoClient;

        private IMongoCollection<UserClassifiedAdd> m_classifiedsCollection;
        private IMongoCollection<UserProfilePick> m_picksCollection;
        private IMongoCollection<UserProfileNotes> m_notesCollection;
        private IMongoCollection<UserProfileProperties> m_profilesCollection;
        private IMongoCollection<UserPreferences> m_preferencesCollection;
        private IMongoCollection<UserAppData> m_appDataCollection;

        public void Initialise(string connectionString)
        {
            m_connectionString = connectionString;

            m_log.Info("[PROFILES_DATA]: MongoDB - connecting: " + m_connectionString);

            try
            {
                m_mongoClient = new MongoClient(connectionString);
                m_mongoDatabase = m_mongoClient.GetDatabase(new MongoUrl(connectionString).DatabaseName);

                m_classifiedsCollection = m_mongoDatabase.GetCollection<UserClassifiedAdd>("classifieds");
                m_picksCollection = m_mongoDatabase.GetCollection<UserProfilePick>("userpicks");
                m_notesCollection = m_mongoDatabase.GetCollection<UserProfileNotes>("usernotes");
                m_profilesCollection = m_mongoDatabase.GetCollection<UserProfileProperties>("userprofile");
                m_preferencesCollection = m_mongoDatabase.GetCollection<UserPreferences>("usersettings");
                m_appDataCollection = m_mongoDatabase.GetCollection<UserAppData>("userdata");
                // Other collections will be initialized as needed during their respective method refactorings.

            }
            catch (Exception ex)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: Error connecting to MongoDB: {0}", ex.Message);
                throw;
            }
        }

        private string[] FieldList
        {
            get { return new List<string>(m_FieldMap.Keys).ToArray(); }
        }

        #region IProfilesData implementation
        public OSDArray GetClassifiedRecords(UUID creatorId)
        {
            OSDArray data = new OSDArray();
            try
            {
                var filter = Builders<UserClassifiedAdd>.Filter.Eq(c => c.CreatorId, creatorId);
                var projection = Builders<UserClassifiedAdd>.Projection.Include(c => c.ClassifiedId).Include(c => c.Name);

                var classifieds = m_classifiedsCollection.Find(filter).Project(projection).ToList();

                foreach (var classified in classifieds)
                {
                    OSDMap n = new OSDMap();
                    UUID Id = UUID.Zero;
                    string Name = null;

                    if (classified.Contains("ClassifiedId"))
                    {
                        UUID.TryParse(classified["ClassifiedId"].ToString(), out Id);
                    }
                    if (classified.Contains("Name"))
                    {
                        Name = classified["Name"].ToString();
                    }

                    n.Add("classifieduuid", OSD.FromUUID(Id));
                    n.Add("name", OSD.FromString(Name));
                    data.Add(n);
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetClassifiedRecords exception {0}", e.Message);
            }
            return data;
        }
        public bool UpdateClassifiedRecord(UserClassifiedAdd ad, ref string result)
        {
            if (string.IsNullOrEmpty(ad.ParcelName))
                ad.ParcelName = "Unknown";
            if (string.IsNullOrEmpty(ad.Description))
                ad.Description = "No Description";

            DateTime epoch = new DateTime(1970, 1, 1);
            DateTime now = DateTime.Now;
            TimeSpan epochnow = now - epoch;
            TimeSpan duration;
            DateTime expiration;
            TimeSpan epochexp;

            if (ad.Flags == 2)
            {
                duration = new TimeSpan(7, 0, 0, 0);
                expiration = now.Add(duration);
                epochexp = expiration - epoch;
            }
            else
            {
                duration = new TimeSpan(365, 0, 0, 0);
                expiration = now.Add(duration);
                epochexp = expiration - epoch;
            }
            ad.CreationDate = (int)epochnow.TotalSeconds;
            ad.ExpirationDate = (int)epochexp.TotalSeconds;

            try
            {
                var filter = Builders<UserClassifiedAdd>.Filter.Eq(c => c.ClassifiedId, ad.ClassifiedId);
                var options = new ReplaceOptions { IsUpsert = true };

                // ReplaceOne will insert if the document does not exist (upsert: true)
                var replaceResult = m_classifiedsCollection.ReplaceOne(filter, ad, options);

                if (!replaceResult.IsAcknowledged || (replaceResult.ModifiedCount == 0 && replaceResult.UpsertedId == null))
                {
                    result = "Failed to update or insert classified record.";
                    return false;
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: ClassifiedsUpdate exception {0}", e.Message);
                result = e.Message;
                return false;
            }
            return true;
        }
        public bool DeleteClassifiedRecord(UUID recordId)
        {
            try
            {
                var filter = Builders<UserClassifiedAdd>.Filter.Eq(c => c.ClassifiedId, recordId);
                var deleteResult = m_classifiedsCollection.DeleteOne(filter);

                return deleteResult.IsAcknowledged && deleteResult.DeletedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: DeleteClassifiedRecord exception {0}", e.Message);
                return false;
            }
        }

        public bool GetClassifiedInfo(ref UserClassifiedAdd ad, ref string result)
        {
            try
            {
                var filter = Builders<UserClassifiedAdd>.Filter.Eq(c => c.ClassifiedId, ad.ClassifiedId);
                var classified = m_classifiedsCollection.Find(filter).FirstOrDefault();

                if (classified != null)
                {
                    ad.CreatorId = classified.CreatorId;
                    ad.ParcelId = classified.ParcelId;
                    ad.SnapshotId = classified.SnapshotId;
                    ad.CreationDate = classified.CreationDate;
                    ad.ExpirationDate = classified.ExpirationDate;
                    ad.ParentEstate = classified.ParentEstate;
                    ad.Flags = classified.Flags;
                    ad.Category = classified.Category;
                    ad.Price = classified.Price;
                    ad.Name = classified.Name;
                    ad.Description = classified.Description;
                    ad.SimName = classified.SimName;
                    ad.GlobalPos = classified.GlobalPos;
                    ad.ParcelName = classified.ParcelName;
                }
                else
                {
                    result = "Classified record not found.";
                    return false;
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetClassifiedInfo exception {0}", e.Message);
                result = e.Message;
                return false;
            }
            return true;
        }

        public OSDArray GetAvatarPicks(UUID avatarId)
        {
            IDataReader reader = null;
            string query = string.Empty;

            query += "SELECT `pickuuid`,`name` FROM userpicks WHERE ";
            query += "creatoruuid = :Id";
            OSDArray data = new OSDArray();

            try
            {
                var filter = Builders<UserProfilePick>.Filter.Eq(p => p.CreatorId, avatarId);
                var projection = Builders<UserProfilePick>.Projection.Include(p => p.PickId).Include(p => p.Name);

                var picks = m_picksCollection.Find(filter).Project(projection).ToList();

                foreach (var pick in picks)
                {
                    OSDMap record = new OSDMap();
                    UUID pickId = UUID.Zero;
                    string name = null;

                    if (pick.Contains("PickId"))
                    {
                        UUID.TryParse(pick["PickId"].ToString(), out pickId);
                    }
                    if (pick.Contains("Name"))
                    {
                        name = pick["Name"].ToString();
                    }

                    record.Add("pickuuid", OSD.FromString(pickId.ToString()));
                    record.Add("name", OSD.FromString(name));
                    data.Add(record);
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetAvatarPicks exception {0}", e.Message);
            }
            return data;
        }
        public UserProfilePick GetPickInfo(UUID avatarId, UUID pickId)
        {
            UserProfilePick pick = new UserProfilePick();
            try
            {
                var filter = Builders<UserProfilePick>.Filter.And(
                    Builders<UserProfilePick>.Filter.Eq(p => p.CreatorId, avatarId),
                    Builders<UserProfilePick>.Filter.Eq(p => p.PickId, pickId)
                );

                pick = m_picksCollection.Find(filter).FirstOrDefault();

                if (pick == null)
                {
                    pick = new UserProfilePick(); // Return an empty pick if not found
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetPickInfo exception {0}", e.Message);
            }
            return pick;
        }

        public bool UpdatePicksRecord(UserProfilePick pick)
        {
            try
            {
                var filter = Builders<UserProfilePick>.Filter.Eq(p => p.PickId, pick.PickId);
                var options = new ReplaceOptions { IsUpsert = true };

                var replaceResult = m_picksCollection.ReplaceOne(filter, pick, options);

                return replaceResult.IsAcknowledged && (replaceResult.ModifiedCount > 0 || replaceResult.UpsertedId != null);
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: UpdateAvatarNotes exception {0}", e.Message);
                return false;
            }
        }

        public bool DeletePicksRecord(UUID pickId)
        {
            try
            {
                var filter = Builders<UserProfilePick>.Filter.Eq(p => p.PickId, pickId);
                var deleteResult = m_picksCollection.DeleteOne(filter);

                return deleteResult.IsAcknowledged && deleteResult.DeletedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: DeleteUserPickRecord exception {0}", e.Message);
                return false;
            }
        }

        public bool GetAvatarNotes(ref UserProfileNotes notes)
        {
            try
            {
                var filter = Builders<UserProfileNotes>.Filter.And(
                    Builders<UserProfileNotes>.Filter.Eq(n => n.UserId, notes.UserId),
                    Builders<UserProfileNotes>.Filter.Eq(n => n.TargetId, notes.TargetId)
                );

                var existingNotes = m_notesCollection.Find(filter).FirstOrDefault();

                if (existingNotes != null)
                {
                    notes.Notes = existingNotes.Notes;
                }
                else
                {
                    notes.Notes = string.Empty; // If no notes found, set to empty string
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetAvatarNotes exception {0}", e.Message);
                return false;
            }
            return true;
        }

        public bool UpdateAvatarNotes(ref UserProfileNotes note, ref string result)
        {
            try
            {
                var filter = Builders<UserProfileNotes>.Filter.And(
                    Builders<UserProfileNotes>.Filter.Eq(n => n.UserId, note.UserId),
                    Builders<UserProfileNotes>.Filter.Eq(n => n.TargetId, note.TargetId)
                );

                if (string.IsNullOrEmpty(note.Notes))
                {
                    // If notes are empty, delete the record
                    var deleteResult = m_notesCollection.DeleteOne(filter);
                    return deleteResult.IsAcknowledged && deleteResult.DeletedCount > 0;
                }
                else
                {
                    // Otherwise, insert or replace the record
                    var options = new ReplaceOptions { IsUpsert = true };
                    var replaceResult = m_notesCollection.ReplaceOne(filter, note, options);
                    return replaceResult.IsAcknowledged && (replaceResult.ModifiedCount > 0 || replaceResult.UpsertedId != null);
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: UpdateAvatarNotes exception {0}", e.Message);
                result = e.Message;
                return false;
            }
        }

        public bool GetAvatarProperties(ref UserProfileProperties props, ref string result)
        {
            try
            {
                var filter = Builders<UserProfileProperties>.Filter.Eq(p => p.UserId, props.UserId);
                var existingProps = m_profilesCollection.Find(filter).FirstOrDefault();

                if (existingProps != null)
                {
                    props.WebUrl = existingProps.WebUrl;
                    props.ImageId = existingProps.ImageId;
                    props.AboutText = existingProps.AboutText;
                    props.FirstLifeImageId = existingProps.FirstLifeImageId;
                    props.FirstLifeText = existingProps.FirstLifeText;
                    props.PartnerId = existingProps.PartnerId;
                    props.WantToMask = existingProps.WantToMask;
                    props.WantToText = existingProps.WantToText;
                    props.SkillsMask = existingProps.SkillsMask;
                    props.SkillsText = existingProps.SkillsText;
                    props.Language = existingProps.Language;
                    props.PublishProfile = existingProps.PublishProfile;
                    props.PublishMature = existingProps.PublishMature;
                }
                else
                {
                    // If no properties found, initialize with default values and insert a new record
                    props.WebUrl = string.Empty;
                    props.ImageId = UUID.Zero;
                    props.AboutText = string.Empty;
                    props.FirstLifeImageId = UUID.Zero;
                    props.FirstLifeText = string.Empty;
                    props.PartnerId = UUID.Zero;
                    props.WantToMask = 0;
                    props.WantToText = string.Empty;
                    props.SkillsMask = 0;
                    props.SkillsText = string.Empty;
                    props.Language = string.Empty;
                    props.PublishProfile = false;
                    props.PublishMature = false;

                    m_profilesCollection.InsertOne(props);
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetAvatarProperties exception {0}", e.Message);
                result = e.Message;
                return false;
            }
            return true;
        }

        public bool UpdateAvatarProperties(ref UserProfileProperties props, ref string result)
        {
            try
            {
                var filter = Builders<UserProfileProperties>.Filter.Eq(p => p.UserId, props.UserId);
                var update = Builders<UserProfileProperties>.Update
                    .Set(p => p.WebUrl, props.WebUrl)
                    .Set(p => p.ImageId, props.ImageId)
                    .Set(p => p.AboutText, props.AboutText)
                    .Set(p => p.FirstLifeImageId, props.FirstLifeImageId)
                    .Set(p => p.FirstLifeText, props.FirstLifeText);

                var updateResult = m_profilesCollection.UpdateOne(filter, update);

                return updateResult.IsAcknowledged && updateResult.ModifiedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: AgentPropertiesUpdate exception {0}", e.Message);
                result = e.Message;
                return false;
            }
        }

        public bool UpdateAvatarInterests(UserProfileProperties up, ref string result)
        {
            try
            {
                var filter = Builders<UserProfileProperties>.Filter.Eq(p => p.UserId, up.UserId);
                var update = Builders<UserProfileProperties>.Update
                    .Set(p => p.WantToMask, up.WantToMask)
                    .Set(p => p.WantToText, up.WantToText)
                    .Set(p => p.SkillsMask, up.SkillsMask)
                    .Set(p => p.SkillsText, up.SkillsText)
                    .Set(p => p.Language, up.Language);

                var updateResult = m_profilesCollection.UpdateOne(filter, update);

                return updateResult.IsAcknowledged && updateResult.ModifiedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: AgentInterestsUpdate exception {0}", e.Message);
                result = e.Message;
                return false;
            }
        }


        public bool UpdateUserPreferences(ref UserPreferences pref, ref string result)
        {
            try
            {
                var filter = Builders<UserPreferences>.Filter.Eq(p => p.UserId, pref.UserId);
                var update = Builders<UserPreferences>.Update
                    .Set(p => p.IMViaEmail, pref.IMViaEmail)
                    .Set(p => p.Visible, pref.Visible)
                    .Set(p => p.EMail, pref.EMail);

                var updateResult = m_preferencesCollection.UpdateOne(filter, update);

                return updateResult.IsAcknowledged && updateResult.ModifiedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: UpdateUserPreferences exception {0}", e.Message);
                result = e.Message;
                return false;
            }
        }

        public bool GetUserPreferences(ref UserPreferences pref, ref string result)
        {
            try
            {
                var filter = Builders<UserPreferences>.Filter.Eq(p => p.UserId, pref.UserId);
                var existingPref = m_preferencesCollection.Find(filter).FirstOrDefault();

                if (existingPref != null)
                {
                    pref.IMViaEmail = existingPref.IMViaEmail;
                    pref.Visible = existingPref.Visible;
                    pref.EMail = existingPref.EMail;
                }
                else
                {
                    // If no preferences found, insert a new record with default values
                    pref.IMViaEmail = false;
                    pref.Visible = false;
                    // Email might be set by other means, so we don't default it here.
                    m_preferencesCollection.InsertOne(pref);
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: Get preferences exception {0}", e.Message);
                result = e.Message;
                return false;
            }
            return true;
        }

        public bool GetUserAppData(ref UserAppData props, ref string result)
        {
            try
            {
                var filter = Builders<UserAppData>.Filter.And(
                    Builders<UserAppData>.Filter.Eq(d => d.UserId, props.UserId),
                    Builders<UserAppData>.Filter.Eq(d => d.TagId, props.TagId)
                );

                var existingAppData = m_appDataCollection.Find(filter).FirstOrDefault();

                if (existingAppData != null)
                {
                    props.DataKey = existingAppData.DataKey;
                    props.DataVal = existingAppData.DataVal;
                }
                else
                {
                    // If no app data found, insert a new record with default values
                    m_appDataCollection.InsertOne(props);
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: Requst application data exception {0}", e.Message);
                result = e.Message;
                return false;
            }
            return true;
        }
        public bool SetUserAppData(UserAppData props, ref string result)
        {
            try
            {
                var filter = Builders<UserAppData>.Filter.And(
                    Builders<UserAppData>.Filter.Eq(d => d.UserId, props.UserId),
                    Builders<UserAppData>.Filter.Eq(d => d.TagId, props.TagId)
                );
                var update = Builders<UserAppData>.Update
                    .Set(d => d.DataKey, props.DataKey)
                    .Set(d => d.DataVal, props.DataVal);

                var updateResult = m_appDataCollection.UpdateOne(filter, update);

                return updateResult.IsAcknowledged && updateResult.ModifiedCount > 0;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: SetUserData exception {0}", e.Message);
                return false;
            }
        }
        public OSDArray GetUserImageAssets(UUID avatarId)
        {
            OSDArray data = new OSDArray();
            try
            {
                // Get classified image assets
                var classifieds = m_classifiedsCollection.Find(Builders<UserClassifiedAdd>.Filter.Eq(c => c.CreatorId, avatarId))
                                                        .Project(Builders<UserClassifiedAdd>.Projection.Include(c => c.SnapshotId))
                                                        .ToList();
                foreach (var classified in classifieds)
                {
                    if (classified.Contains("SnapshotId"))
                    {
                        data.Add(OSD.FromString(classified["SnapshotId"].ToString()));
                    }
                }

                // Get pick image assets
                var picks = m_picksCollection.Find(Builders<UserProfilePick>.Filter.Eq(p => p.CreatorId, avatarId))
                                            .Project(Builders<UserProfilePick>.Projection.Include(p => p.SnapshotId))
                                            .ToList();
                foreach (var pick in picks)
                {
                    if (pick.Contains("SnapshotId"))
                    {
                        data.Add(OSD.FromString(pick["SnapshotId"].ToString()));
                    }
                }

                // Get profile images
                var profile = m_profilesCollection.Find(Builders<UserProfileProperties>.Filter.Eq(p => p.UserId, avatarId))
                                                .Project(Builders<UserProfileProperties>.Projection.Include(p => p.ImageId).Include(p => p.FirstLifeImageId))
                                                .FirstOrDefault();
                if (profile != null)
                {
                    if (profile.Contains("ImageId"))
                    {
                        data.Add(OSD.FromString(profile["ImageId"].ToString()));
                    }
                    if (profile.Contains("FirstLifeImageId"))
                    {
                        data.Add(OSD.FromString(profile["FirstLifeImageId"].ToString()));
                    }
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[PROFILES_DATA]: GetUserImageAssets exception {0}", e.Message);
            }
            return data;
        }
        #endregion
    }
}

