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
using System.Drawing;
using System.Reflection;
using System.Threading.Tasks;
using log4net;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace OpenSim.Data.MongoDB
{
    public class PrimDocument
    {
        [BsonId]
        public Guid UUID { get; set; }
        public Guid RegionUUID { get; set; }
        public int CreationDate { get; set; }
        public string Name { get; set; }
        public Guid SceneGroupID { get; set; }
        public string Text { get; set; }
        public int ColorR { get; set; }
        public int ColorG { get; set; }
        public int ColorB { get; set; }
        public int ColorA { get; set; }
        public string Description { get; set; }
        public string SitName { get; set; }
        public string TouchName { get; set; }
        public uint ObjectFlags { get; set; }
        public string CreatorID { get; set; }
        public Guid OwnerID { get; set; }
        public Guid GroupID { get; set; }
        public Guid LastOwnerID { get; set; }
        public Guid RezzerID { get; set; }
        public uint OwnerMask { get; set; }
        public uint NextOwnerMask { get; set; }
        public uint GroupMask { get; set; }
        public uint EveryoneMask { get; set; }
        public uint BaseMask { get; set; }
        public double PositionX { get; set; }
        public double PositionY { get; set; }
        public double PositionZ { get; set; }
        public double GroupPositionX { get; set; }
        public double GroupPositionY { get; set; }
        public double GroupPositionZ { get; set; }
        public double VelocityX { get; set; }
        public double VelocityY { get; set; }
        public double VelocityZ { get; set; }
        public double AngularVelocityX { get; set; }
        public double AngularVelocityY { get; set; }
        public double AngularVelocityZ { get; set; }
        public double AccelerationX { get; set; }
        public double AccelerationY { get; set; }
        public double AccelerationZ { get; set; }
        public double RotationX { get; set; }
        public double RotationY { get; set; }
        public double RotationZ { get; set; }
        public double RotationW { get; set; }
        public double SitTargetOffsetX { get; set; }
        public double SitTargetOffsetY { get; set; }
        public double SitTargetOffsetZ { get; set; }
        public double SitTargetOrientW { get; set; }
        public double SitTargetOrientX { get; set; }
        public double SitTargetOrientY { get; set; }
        public double SitTargetOrientZ { get; set; }
        public int PayPrice { get; set; }
        public int PayButton1 { get; set; }
        public int PayButton2 { get; set; }
        public int PayButton3 { get; set; }
        public int PayButton4 { get; set; }
        public string LoopedSound { get; set; }
        public double LoopedSoundGain { get; set; }
        public string TextureAnimation { get; set; }
        public string ParticleSystem { get; set; }
        public double CameraEyeOffsetX { get; set; }
        public double CameraEyeOffsetY { get; set; }
        public double CameraEyeOffsetZ { get; set; }
        public double CameraAtOffsetX { get; set; }
        public double CameraAtOffsetY { get; set; }
        public double CameraAtOffsetZ { get; set; }
        public short ForceMouselook { get; set; }
        public int ScriptAccessPin { get; set; }
        public short AllowedDrop { get; set; }
        public short DieAtEdge { get; set; }
        public int SalePrice { get; set; }
        public short SaleType { get; set; }
        public byte ClickAction { get; set; }
        public byte Material { get; set; }
        public string CollisionSound { get; set; }
        public double CollisionSoundVolume { get; set; }
        public short VolumeDetect { get; set; }
        public string MediaURL { get; set; }
        public double AttachedPosX { get; set; }
        public double AttachedPosY { get; set; }
        public double AttachedPosZ { get; set; }
        public string DynAttrs { get; set; }
        public byte PhysicsShapeType { get; set; }
        public double Density { get; set; }
        public double GravityModifier { get; set; }
        public double Friction { get; set; }
        public double Restitution { get; set; }
        public byte[] KeyframeMotion { get; set; }
        public bool PassTouches { get; set; }
        public bool PassCollisions { get; set; }
        public byte RotationAxisLocks { get; set; }
        public string Vehicle { get; set; }
        public string PhysInertia { get; set; }
        public float standtargetx { get; set; }
        public float standtargety { get; set; }
        public float standtargetz { get; set; }
        public float sitactrange { get; set; }
        public int pseudocrc { get; set; }
        public byte[] sopanims { get; set; }
        public byte[] lnkstBinData { get; set; }
        public string StartStr { get; set; }
    }

    public class ShapeDocument
    {
        [BsonId]
        public Guid UUID { get; set; }
        public int Shape { get; set; }
        public double ScaleX { get; set; }
        public double ScaleY { get; set; }
        public double ScaleZ { get; set; }
        public int PCode { get; set; }
        public int PathBegin { get; set; }
        public int PathEnd { get; set; }
        public int PathScaleX { get; set; }
        public int PathScaleY { get; set; }
        public int PathShearX { get; set; }
        public int PathShearY { get; set; }
        public int PathSkew { get; set; }
        public int PathCurve { get; set; }
        public int PathRadiusOffset { get; set; }
        public int PathRevolutions { get; set; }
        public int PathTaperX { get; set; }
        public int PathTaperY { get; set; }
        public int PathTwist { get; set; }
        public int PathTwistBegin { get; set; }
        public int ProfileBegin { get; set; }
        public int ProfileEnd { get; set; }
        public int ProfileCurve { get; set; }
        public int ProfileHollow { get; set; }
        public int State { get; set; }
        public int LastAttachPoint { get; set; }
        public byte[] Texture { get; set; }
        public byte[] ExtraParams { get; set; }
        public string Media { get; set; }
        public byte[] MatOvrd { get; set; }
    }

    public class TerrainDocument
    {
        [BsonId]
        public Guid RegionUUID { get; set; }
        public int Revision { get; set; }
        public byte[] Heightfield { get; set; }
    }

    public class BakedTerrainDocument
    {
        [BsonId]
        public Guid RegionUUID { get; set; }
        public int Revision { get; set; }
        public byte[] Heightfield { get; set; }
    }

    public class ItemDocument
    {
        [BsonId]
        public Guid itemID { get; set; }
        public Guid primID { get; set; }
        public Guid assetID { get; set; }
        public Guid parentFolderID { get; set; }
        public int invType { get; set; }
        public int assetType { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public long creationDate { get; set; }
        public string creatorID { get; set; }
        public Guid ownerID { get; set; }
        public Guid lastOwnerID { get; set; }
        public Guid groupID { get; set; }
        public uint nextPermissions { get; set; }
        public uint currentPermissions { get; set; }
        public uint basePermissions { get; set; }
        public uint everyonePermissions { get; set; }
        public uint groupPermissions { get; set; }
        public uint flags { get; set; }
    }

    public class LandDocument
    {
        [BsonId]
        public Guid UUID { get; set; }
        public Guid RegionUUID { get; set; }
        public uint LocalLandID { get; set; }
        public byte[] Bitmap { get; set; }
        public string Name { get; set; }
        public string Desc { get; set; }
        public Guid OwnerUUID { get; set; }
        public bool IsGroupOwned { get; set; }
        public int Area { get; set; }
        public int AuctionID { get; set; }
        public int Category { get; set; }
        public int ClaimDate { get; set; }
        public int ClaimPrice { get; set; }
        public Guid GroupUUID { get; set; }
        public int SalePrice { get; set; }
        public int LandStatus { get; set; }
        public uint LandFlags { get; set; }
        public byte LandingType { get; set; }
        public byte MediaAutoScale { get; set; }
        public Guid MediaTextureUUID { get; set; }
        public string MediaURL { get; set; }
        public string MusicURL { get; set; }
        public double PassHours { get; set; }
        public uint PassPrice { get; set; }
        public Guid SnapshotUUID { get; set; }
        public double UserLocationX { get; set; }
        public double UserLocationY { get; set; }
        public double UserLocationZ { get; set; }
        public double UserLookAtX { get; set; }
        public double UserLookAtY { get; set; }
        public double UserLookAtZ { get; set; }
        public Guid AuthbuyerID { get; set; }
        public int OtherCleanTime { get; set; }
        public int Dwell { get; set; }
        public string MediaType { get; set; }
        public string MediaDescription { get; set; }
        public string MediaSize { get; set; }
        public bool MediaLoop { get; set; }
        public bool ObscureMedia { get; set; }
        public bool ObscureMusic { get; set; }
        public bool SeeAVs { get; set; }
        public bool AnyAVSounds { get; set; }
        public bool GroupAVSounds { get; set; }
        public string environment { get; set; }
    }

    public class LandAccessDocument
    {
        [BsonId]
        public ObjectId Id { get; set; } // MongoDB's default ObjectId
        public Guid LandUUID { get; set; }
        public Guid AccessUUID { get; set; }
        public uint Flags { get; set; }
    }

    public class RegionSettingsDocument
    {
        [BsonId]
        public Guid regionUUID { get; set; }
        public int block_terraform { get; set; }
        public int block_fly { get; set; }
        public int allow_damage { get; set; }
        public int restrict_pushing { get; set; }
        public int allow_land_resell { get; set; }
        public int allow_land_join_divide { get; set; }
        public int block_show_in_search { get; set; }
        public int agent_limit { get; set; }
        public double object_bonus { get; set; }
        public int maturity { get; set; }
        public int disable_scripts { get; set; }
        public int disable_collisions { get; set; }
        public int disable_physics { get; set; }
        public string terrain_texture_1 { get; set; }
        public string terrain_texture_2 { get; set; }
        public string terrain_texture_3 { get; set; }
        public string terrain_texture_4 { get; set; }
        public string TerrainPBR1 { get; set; }
        public string TerrainPBR2 { get; set; }
        public string TerrainPBR3 { get; set; }
        public string TerrainPBR4 { get; set; }
        public double elevation_1_nw { get; set; }
        public double elevation_2_nw { get; set; }
        public double elevation_1_ne { get; set; }
        public double elevation_2_ne { get; set; }
        public double elevation_1_se { get; set; }
        public double elevation_2_se { get; set; }
        public double elevation_1_sw { get; set; }
        public double elevation_2_sw { get; set; }
        public double water_height { get; set; }
        public double terrain_raise_limit { get; set; }
        public double terrain_lower_limit { get; set; }
        public int use_estate_sun { get; set; }
        public int sandbox { get; set; }
        public double sunvectorx { get; set; }
        public double sunvectory { get; set; }
        public double sunvectorz { get; set; }
        public int fixed_sun { get; set; }
        public double sun_position { get; set; }
        public string covenant { get; set; }
        public int covenant_datetime { get; set; }
        public string map_tile_ID { get; set; }
        public string TelehubObject { get; set; }
        public string parcel_tile_ID { get; set; }
        public bool block_search { get; set; }
        public bool casino { get; set; }
        public string cacheID { get; set; }
    }

    public class RegionEnvironmentDocument
    {
        [BsonId]
        public Guid region_id { get; set; }
        public string llsd_settings { get; set; }
    }
    
    public class ExtraDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }
        public Guid RegionID { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
    }

    /// <summary>
    /// A RegionData Interface to the MongoDB database
    /// </summary>
    public class MongoDBSimulationData : ISimulationDataStore
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private MongoClient m_client;
        private IMongoDatabase m_database;

        private IMongoCollection<PrimDocument> m_primsCollection;
        private IMongoCollection<ShapeDocument> m_shapesCollection;
        private IMongoCollection<ItemDocument> m_itemsCollection;
        private IMongoCollection<TerrainDocument> m_terrainCollection;
        private IMongoCollection<BakedTerrainDocument> m_bakedTerrainCollection;
        private IMongoCollection<LandDocument> m_landCollection;
        private IMongoCollection<LandAccessDocument> m_landAccessListCollection;
        private IMongoCollection<RegionSettingsDocument> m_regionSettingsCollection;
        private IMongoCollection<RegionEnvironmentDocument> m_regionEnvironmentCollection;
        private IMongoCollection<ExtraDocument> m_extraCollection;

        public void Initialise(string connectionString)
        {
            try
            {
                m_log.InfoFormat("[MONGODB SIMULATION]: Connecting to {0}", connectionString);
                var settings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
                m_client = new MongoClient(settings);
                var url = new MongoUrl(connectionString);
                m_database = m_client.GetDatabase(url.DatabaseName ?? "opensim");

                m_primsCollection = m_database.GetCollection<PrimDocument>("prims");
                m_shapesCollection = m_database.GetCollection<ShapeDocument>("primshapes");
                m_itemsCollection = m_database.GetCollection<ItemDocument>("primitems");
                m_terrainCollection = m_database.GetCollection<TerrainDocument>("terrain");
                m_bakedTerrainCollection = m_database.GetCollection<BakedTerrainDocument>("bakedterrain");
                m_landCollection = m_database.GetCollection<LandDocument>("land");
                m_landAccessListCollection = m_database.GetCollection<LandAccessDocument>("landaccesslist");
                m_regionSettingsCollection = m_database.GetCollection<RegionSettingsDocument>("regionsettings");
                m_regionEnvironmentCollection = m_database.GetCollection<RegionEnvironmentDocument>("regionenvironment");
                m_extraCollection = m_database.GetCollection<ExtraDocument>("region_extra");

                // Create indexes for faster queries
                m_primsCollection.Indexes.CreateOne(new CreateIndexModel<PrimDocument>(Builders<PrimDocument>.IndexKeys.Ascending(p => p.RegionUUID)));
                m_itemsCollection.Indexes.CreateOne(new CreateIndexModel<ItemDocument>(Builders<ItemDocument>.IndexKeys.Ascending(i => i.primID)));
                m_landCollection.Indexes.CreateOne(new CreateIndexModel<LandDocument>(Builders<LandDocument>.IndexKeys.Ascending(l => l.RegionUUID)));
                m_landAccessListCollection.Indexes.CreateOne(new CreateIndexModel<LandAccessDocument>(Builders<LandAccessDocument>.IndexKeys.Ascending(la => la.LandUUID)));
            }
            catch (Exception e)
            {
                m_log.Fatal($"[MONGODB SIMULATION]: Couldn't connect to database: {e.Message}", e);
                throw;
            }
        }

        public void Dispose() { }

        public void Shutdown() { }

        public void StoreObject(SceneObjectGroup obj, UUID regionUUID)
        {
            foreach (SceneObjectPart prim in obj.Parts)
            {
                var primDoc = ConvertToPrimDocument(prim, obj.UUID, regionUUID);
                m_primsCollection.ReplaceOne(p => p.UUID == prim.UUID, primDoc, new ReplaceOptions { IsUpsert = true });

                var shapeDoc = ConvertToShapeDocument(prim.Shape, prim.UUID);
                m_shapesCollection.ReplaceOne(s => s.UUID == prim.UUID, shapeDoc, new ReplaceOptions { IsUpsert = true });
            }
        }

        public void RemoveObject(UUID uuid, UUID regionUUID)
        {
            var primsInObject = m_primsCollection.Find(p => p.SceneGroupID == uuid && p.RegionUUID == regionUUID).ToList();
            foreach (var primDoc in primsInObject)
            {
                m_shapesCollection.DeleteOne(s => s.UUID == primDoc.UUID);
                m_itemsCollection.DeleteMany(i => i.primID == primDoc.UUID);
            }
            m_primsCollection.DeleteMany(p => p.SceneGroupID == uuid && p.RegionUUID == regionUUID);
        }

        public void StorePrimInventory(UUID primID, ICollection<TaskInventoryItem> items)
        {
            m_itemsCollection.DeleteMany(i => i.primID == primID);

            if (items != null && items.Count > 0)
            {
                var itemDocs = new List<ItemDocument>();
                foreach (var item in items)
                {
                    itemDocs.Add(ConvertToItemDocument(item));
                }
                m_itemsCollection.InsertMany(itemDocs);
            }
        }

        public List<SceneObjectGroup> LoadObjects(UUID regionUUID)
        {
            var prims = m_primsCollection.Find(p => p.RegionUUID == regionUUID).ToList();
            if (prims.Count == 0)
                return new List<SceneObjectGroup>();

            var primIds = new List<Guid>();
            foreach(var p in prims)
                primIds.Add(p.UUID);

            var shapes = m_shapesCollection.Find(s => primIds.Contains(s.UUID)).ToList().ToDictionary(s => s.UUID);
            var items = m_itemsCollection.Find(i => primIds.Contains(i.primID)).ToList();

            var objects = new Dictionary<Guid, SceneObjectGroup>();
            var primsToSOG = new Dictionary<Guid, SceneObjectGroup>();

            foreach (var primDoc in prims)
            {
                SceneObjectPart part = ConvertFromPrimDocument(primDoc);
                
                if (shapes.TryGetValue(primDoc.UUID, out var shapeDoc))
                {
                    part.Shape = ConvertFromShapeDocument(shapeDoc);
                }
                else
                {
                    m_log.Warn($"[MONGODB SIMULATION]: Missing shape for prim {primDoc.UUID}");
                    part.Shape = new PrimitiveBaseShape();
                }

                if (objects.TryGetValue(primDoc.SceneGroupID, out var sog))
                {
                    sog.AddPart(part);
                }
                else
                {
                    sog = new SceneObjectGroup(part);
                    objects.Add(primDoc.SceneGroupID, sog);
                }
                primsToSOG.Add(part.UUID, sog);
            }
            
            foreach (var itemDoc in items)
            {
                if (primsToSOG.TryGetValue(itemDoc.primID, out var sog))
                {
                    var part = sog.GetPart(itemDoc.primID);
                    if (part != null)
                    {
                        part.Inventory.RestoreItem(ConvertFromItemDocument(itemDoc));
                    }
                }
            }

            return new List<SceneObjectGroup>(objects.Values);
        }

        public void StoreTerrain(TerrainData terrain, UUID regionID)
        {
            var doc = new TerrainDocument
            {
                RegionUUID = regionID,
                Revision = terrain.Revision,
                Heightfield = terrain.GetRawTerrain()
            };
            m_terrainCollection.ReplaceOne(t => t.RegionUUID == regionID, doc, new ReplaceOptions { IsUpsert = true });
        }

        public void StoreBakedTerrain(TerrainData terrain, UUID regionID)
        {
             var doc = new BakedTerrainDocument
            {
                RegionUUID = regionID,
                Revision = terrain.Revision,
                Heightfield = terrain.GetRawTerrain()
            };
            m_bakedTerrainCollection.ReplaceOne(t => t.RegionUUID == regionID, doc, new ReplaceOptions { IsUpsert = true });
        }

        public void StoreTerrain(double[,] terrain, UUID regionID)
        {
            var tdata = new TerrainData(terrain);
            StoreTerrain(tdata, regionID);
        }

        public TerrainData LoadTerrain(UUID regionID, int pSizeX, int pSizeY, int pSizeZ)
        {
            var doc = m_terrainCollection.Find(t => t.RegionUUID == regionID).FirstOrDefault();
            if (doc != null)
            {
                var tdata = new TerrainData(pSizeX, pSizeY, pSizeZ, doc.Heightfield);
                tdata.Revision = doc.Revision;
                return tdata;
            }
            return null;
        }
        
        public TerrainData LoadBakedTerrain(UUID regionID, int pSizeX, int pSizeY, int pSizeZ)
        {
            var doc = m_bakedTerrainCollection.Find(t => t.RegionUUID == regionID).FirstOrDefault();
            if (doc != null)
            {
                var tdata = new TerrainData(pSizeX, pSizeY, pSizeZ, doc.Heightfield);
                tdata.Revision = doc.Revision;
                return tdata;
            }
            return null;
        }

        public double[,] LoadTerrain(UUID regionID)
        {
            var tdata = LoadTerrain(regionID, (int)Constants.RegionSize, (int)Constants.RegionSize, (int)Constants.MaxRegionSize);
            return tdata?.GetDoubles();
        }

        public void StoreLandObject(ILandObject parcel)
        {
            var landDoc = ConvertToLandDocument(parcel.LandData, parcel.RegionUUID);
            m_landCollection.ReplaceOne(l => l.UUID == landDoc.UUID, landDoc, new ReplaceOptions { IsUpsert = true });

            m_landAccessListCollection.DeleteMany(la => la.LandUUID == parcel.LandData.GlobalID);
            if (parcel.LandData.ParcelAccessList.Count > 0)
            {
                var accessDocs = new List<LandAccessDocument>();
                foreach (var entry in parcel.LandData.ParcelAccessList)
                {
                    accessDocs.Add(ConvertToLandAccessDocument(entry, parcel.LandData.GlobalID));
                }
                m_landAccessListCollection.InsertMany(accessDocs);
            }
        }

        public void RemoveLandObject(UUID globalID)
        {
            m_landCollection.DeleteOne(l => l.UUID == globalID.Guid);
            m_landAccessListCollection.DeleteMany(la => la.LandUUID == globalID.Guid);
        }

        public List<LandData> LoadLandObjects(UUID regionUUID)
        {
            var landDocs = m_landCollection.Find(l => l.RegionUUID == regionUUID.Guid).ToList();
            var landDataList = new List<LandData>();

            foreach (var landDoc in landDocs)
            {
                var landData = ConvertFromLandDocument(landDoc);
                var accessDocs = m_landAccessListCollection.Find(la => la.LandUUID == landData.GlobalID.Guid).ToList();
                foreach (var accessDoc in accessDocs)
                {
                    landData.ParcelAccessList.Add(ConvertFromLandAccessDocument(accessDoc));
                }
                landDataList.Add(landData);
            }
            return landDataList;
        }

        public void StoreRegionSettings(RegionSettings rs)
        {
            var doc = ConvertToRegionSettingsDocument(rs);
            m_regionSettingsCollection.ReplaceOne(r => r.regionUUID == rs.RegionUUID.Guid, doc, new ReplaceOptions { IsUpsert = true });
        }

        public RegionSettings LoadRegionSettings(UUID regionUUID)
        {
            var doc = m_regionSettingsCollection.Find(r => r.regionUUID == regionUUID.Guid).FirstOrDefault();
            return doc != null ? ConvertFromRegionSettingsDocument(doc) : new RegionSettings(regionUUID.Guid);
        }

        public UUID[] GetObjectIDs(UUID regionID)
        {
            var filter = Builders<PrimDocument>.Filter.Eq(p => p.RegionUUID, regionID.Guid);
            var prims = m_primsCollection.Find(filter).ToList();
            var ids = new List<UUID>();
            foreach(var p in prims)
                if(!ids.Contains(p.SceneGroupID))
                    ids.Add(p.SceneGroupID);
            return ids.ToArray();
        }

        public string LoadRegionEnvironmentSettings(UUID regionUUID)
        {
            var doc = m_regionEnvironmentCollection.Find(e => e.region_id == regionUUID).FirstOrDefault();
            return doc?.llsd_settings;
        }

        public void StoreRegionEnvironmentSettings(UUID regionUUID, string settings)
        {
            var doc = new RegionEnvironmentDocument { region_id = regionUUID, llsd_settings = settings };
            m_regionEnvironmentCollection.ReplaceOne(e => e.region_id == regionUUID, doc, new ReplaceOptions { IsUpsert = true });
        }

        public void RemoveRegionEnvironmentSettings(UUID regionUUID)
        {
            m_regionEnvironmentCollection.DeleteOne(e => e.region_id == regionUUID);
        }

        public void SaveExtra(UUID regionID, string name, string val)
        {
            var filter = Builders<ExtraDocument>.Filter.And(
                Builders<ExtraDocument>.Filter.Eq(e => e.RegionID, regionID),
                Builders<ExtraDocument>.Filter.Eq(e => e.Name, name)
            );
            var doc = new ExtraDocument { RegionID = regionID, Name = name, Value = val };
            m_extraCollection.ReplaceOne(filter, doc, new ReplaceOptions { IsUpsert = true });
        }

        public void RemoveExtra(UUID regionID, string name)
        {
             var filter = Builders<ExtraDocument>.Filter.And(
                Builders<ExtraDocument>.Filter.Eq(e => e.RegionID, regionID),
                Builders<ExtraDocument>.Filter.Eq(e => e.Name, name)
            );
            m_extraCollection.DeleteOne(filter);
        }

        public Dictionary<string, string> GetExtra(UUID regionID)
        {
            var extras = m_extraCollection.Find(e => e.RegionID == regionID).ToList();
            var result = new Dictionary<string, string>();
            foreach (var extra in extras)
            {
                result[extra.Name] = extra.Value;
            }
            return result;
        }

        // ===================================================================
        // Conversion helpers
        // ===================================================================

        private PrimDocument ConvertToPrimDocument(SceneObjectPart prim, UUID sceneGroupID, UUID regionUUID)
        {
            return new PrimDocument
            {
                UUID = prim.UUID,
                RegionUUID = regionUUID,
                CreationDate = prim.CreationDate,
                Name = prim.Name,
                SceneGroupID = sceneGroupID,
                Text = prim.Text,
                Description = prim.Description,
                SitName = prim.SitName,
                TouchName = prim.TouchName,
                ObjectFlags = (uint)prim.Flags,
                CreatorID = prim.CreatorIdentification,
                OwnerID = prim.OwnerID,
                GroupID = prim.GroupID,
                LastOwnerID = prim.LastOwnerID,
                RezzerID = prim.RezzerID,
                OwnerMask = prim.OwnerMask,
                NextOwnerMask = prim.NextOwnerMask,
                GroupMask = prim.GroupMask,
                EveryoneMask = prim.EveryoneMask,
                BaseMask = prim.BaseMask,
                PositionX = prim.OffsetPosition.X,
                PositionY = prim.OffsetPosition.Y,
                PositionZ = prim.OffsetPosition.Z,
                GroupPositionX = prim.GroupPosition.X,
                GroupPositionY = prim.GroupPosition.Y,
                GroupPositionZ = prim.GroupPosition.Z,
                VelocityX = prim.Velocity.X,
                VelocityY = prim.Velocity.Y,
                VelocityZ = prim.Velocity.Z,
                AngularVelocityX = prim.AngularVelocity.X,
                AngularVelocityY = prim.AngularVelocity.Y,
                AngularVelocityZ = prim.AngularVelocity.Z,
                AccelerationX = prim.Acceleration.X,
                AccelerationY = prim.Acceleration.Y,
                AccelerationZ = prim.Acceleration.Z,
                RotationX = prim.RotationOffset.X,
                RotationY = prim.RotationOffset.Y,
                RotationZ = prim.RotationOffset.Z,
                RotationW = prim.RotationOffset.W,
                SitTargetOffsetX = prim.SitTargetPositionLL.X,
                SitTargetOffsetY = prim.SitTargetPositionLL.Y,
                SitTargetOffsetZ = prim.SitTargetPositionLL.Z,
                SitTargetOrientW = prim.SitTargetOrientationLL.W,
                SitTargetOrientX = prim.SitTargetOrientationLL.X,
                SitTargetOrientY = prim.SitTargetOrientationLL.Y,
                SitTargetOrientZ = prim.SitTargetOrientationLL.Z,
                standtargetx = prim.StandOffset.X,
                standtargety = prim.StandOffset.Y,
                standtargetz = prim.StandOffset.Z,
                sitactrange = prim.SitActiveRange,
                ColorR = prim.Color.R,
                ColorG = prim.Color.G,
                ColorB = prim.Color.B,
                ColorA = prim.Color.A,
                PayPrice = prim.PayPrice[0],
                PayButton1 = prim.PayPrice[1],
                PayButton2 = prim.PayPrice[2],
                PayButton3 = prim.PayPrice[3],
                PayButton4 = prim.PayPrice[4],
                TextureAnimation = Convert.ToBase64String(prim.TextureAnimation),
                ParticleSystem = Convert.ToBase64String(prim.ParticleSystem),
                CameraEyeOffsetX = prim.GetCameraEyeOffset().X,
                CameraEyeOffsetY = prim.GetCameraEyeOffset().Y,
                CameraEyeOffsetZ = prim.GetCameraEyeOffset().Z,
                CameraAtOffsetX = prim.GetCameraAtOffset().X,
                CameraAtOffsetY = prim.GetCameraAtOffset().Y,
                CameraAtOffsetZ = prim.GetCameraAtOffset().Z,
                LoopedSound = ((prim.SoundFlags & 1) != 0) ? prim.Sound.ToString() : UUID.Zero.ToString(),
                LoopedSoundGain = ((prim.SoundFlags & 1) != 0) ? prim.SoundGain : 0.0f,
                ForceMouselook = (short)(prim.GetForceMouselook() ? 1 : 0),
                ScriptAccessPin = prim.ScriptAccessPin,
                AllowedDrop = (short)(prim.AllowedDrop ? 1 : 0),
                DieAtEdge = (short)(prim.DIE_AT_EDGE ? 1 : 0),
                SalePrice = prim.SalePrice,
                SaleType = (short)prim.ObjectSaleType,
                ClickAction = prim.ClickAction,
                Material = prim.Material,
                CollisionSound = prim.CollisionSound.ToString(),
                CollisionSoundVolume = prim.CollisionSoundVolume,
                VolumeDetect = (short)(prim.VolumeDetectActive ? 1 : 0),
                MediaURL = prim.MediaUrl,
                AttachedPosX = prim.AttachedPos.X,
                AttachedPosY = prim.AttachedPos.Y,
                AttachedPosZ = prim.AttachedPos.Z,
                DynAttrs = (prim.DynAttrs != null && prim.DynAttrs.CountNamespaces > 0) ? prim.DynAttrs.ToXml() : null,
                PhysicsShapeType = prim.PhysicsShapeType,
                Density = prim.Density,
                GravityModifier = prim.GravityModifier,
                Friction = prim.Friction,
                Restitution = prim.Restitution,
                KeyframeMotion = (prim.KeyframeMotion != null) ? prim.KeyframeMotion.Serialize() : Array.Empty<byte>(),
                PassTouches = prim.PassTouches,
                PassCollisions = prim.PassCollisions,
                RotationAxisLocks = prim.RotationAxisLocks,
                Vehicle = (prim.VehicleParams != null) ? prim.VehicleParams.ToXml2() : String.Empty,
                PhysInertia = (prim.PhysicsInertia != null) ? prim.PhysicsInertia.ToXml2() : String.Empty,
                pseudocrc = prim.PseudoCRC,
                sopanims = prim.SerializeAnimations(),
                lnkstBinData = (prim.IsRoot && prim.ParentGroup.LinksetData is not null) ? prim.ParentGroup.LinksetData.ToBin() : null,
                StartStr = prim.IsRoot ? prim.ParentGroup.RezStringParameter : null
            };
        }

        private SceneObjectPart ConvertFromPrimDocument(PrimDocument primDoc)
        {
            var prim = new SceneObjectPart
            {
                UUID = primDoc.UUID,
                CreationDate = primDoc.CreationDate,
                Name = primDoc.Name,
                Text = primDoc.Text,
                Color = Color.FromArgb(primDoc.ColorA, primDoc.ColorR, primDoc.ColorG, primDoc.ColorB),
                Description = primDoc.Description,
                SitName = primDoc.SitName,
                TouchName = primDoc.TouchName,
                Flags = (PrimFlags)primDoc.ObjectFlags,
                CreatorIdentification = primDoc.CreatorID,
                OwnerID = primDoc.OwnerID,
                GroupID = primDoc.GroupID,
                LastOwnerID = primDoc.LastOwnerID,
                RezzerID = primDoc.RezzerID,
                OwnerMask = primDoc.OwnerMask,
                NextOwnerMask = primDoc.NextOwnerMask,
                GroupMask = primDoc.GroupMask,
                EveryoneMask = primDoc.EveryoneMask,
                BaseMask = primDoc.BaseMask,
                OffsetPosition = new Vector3((float)primDoc.PositionX, (float)primDoc.PositionY, (float)primDoc.PositionZ),
                GroupPosition = new Vector3((float)primDoc.GroupPositionX, (float)primDoc.GroupPositionY, (float)primDoc.GroupPositionZ),
                Velocity = new Vector3((float)primDoc.VelocityX, (float)primDoc.VelocityY, (float)primDoc.VelocityZ),
                AngularVelocity = new Vector3((float)primDoc.AngularVelocityX, (float)primDoc.AngularVelocityY, (float)primDoc.AngularVelocityZ),
                Acceleration = new Vector3((float)primDoc.AccelerationX, (float)primDoc.AccelerationY, (float)primDoc.AccelerationZ),
                RotationOffset = new Quaternion((float)primDoc.RotationX, (float)primDoc.RotationY, (float)primDoc.RotationZ, (float)primDoc.RotationW),
                SitTargetPositionLL = new Vector3((float)primDoc.SitTargetOffsetX, (float)primDoc.SitTargetOffsetY, (float)primDoc.SitTargetOffsetZ),
                SitTargetOrientationLL = new Quaternion((float)primDoc.SitTargetOrientX, (float)primDoc.SitTargetOrientY, (float)primDoc.SitTargetOrientZ, (float)primDoc.SitTargetOrientW),
                StandOffset = new Vector3(primDoc.standtargetx, primDoc.standtargety, primDoc.standtargetz),
                SitActiveRange = primDoc.sitactrange,
                ClickAction = primDoc.ClickAction,
                PayPrice = new int[] { primDoc.PayPrice, primDoc.PayButton1, primDoc.PayButton2, primDoc.PayButton3, primDoc.PayButton4 },
                Sound = new UUID(primDoc.LoopedSound),
                SoundGain = (float)primDoc.LoopedSoundGain,
                SoundFlags = string.IsNullOrEmpty(primDoc.LoopedSound) || new UUID(primDoc.LoopedSound).IsZero() ? 0 : 1,
                TextureAnimation = Convert.FromBase64String(primDoc.TextureAnimation ?? ""),
                ParticleSystem = Convert.FromBase64String(primDoc.ParticleSystem ?? ""),
                ScriptAccessPin = primDoc.ScriptAccessPin,
                AllowedDrop = primDoc.AllowedDrop != 0,
                DIE_AT_EDGE = primDoc.DieAtEdge != 0,
                SalePrice = primDoc.SalePrice,
                ObjectSaleType = (byte)primDoc.SaleType,
                Material = primDoc.Material,
                CollisionSound = new UUID(primDoc.CollisionSound),
                CollisionSoundVolume = (float)primDoc.CollisionSoundVolume,
                VolumeDetectActive = primDoc.VolumeDetect != 0,
                MediaUrl = primDoc.MediaURL,
                AttachedPos = new Vector3((float)primDoc.AttachedPosX, (float)primDoc.AttachedPosY, (float)primDoc.AttachedPosZ),
                DynAttrs = string.IsNullOrEmpty(primDoc.DynAttrs) ? null : DAMap.FromXml(primDoc.DynAttrs),
                PhysicsShapeType = primDoc.PhysicsShapeType,
                Density = (float)primDoc.Density,
                GravityModifier = (float)primDoc.GravityModifier,
                Friction = (float)primDoc.Friction,
                Restitution = (float)primDoc.Restitution,
                KeyframeMotion = primDoc.KeyframeMotion != null && primDoc.KeyframeMotion.Length > 0 ? KeyframeMotion.FromData(null, primDoc.KeyframeMotion) : null,
                PassCollisions = primDoc.PassCollisions,
                PassTouches = primDoc.PassTouches,
                RotationAxisLocks = primDoc.RotationAxisLocks,
                VehicleParams = string.IsNullOrEmpty(primDoc.Vehicle) ? null : SOPVehicle.FromXml2(primDoc.Vehicle),
                PhysicsInertia = string.IsNullOrEmpty(primDoc.PhysInertia) ? null : PhysicsInertiaData.FromXml2(primDoc.PhysInertia),
                PseudoCRC = primDoc.pseudocrc
            };
            prim.SetCameraEyeOffset(new Vector3((float)primDoc.CameraEyeOffsetX, (float)primDoc.CameraEyeOffsetY, (float)primDoc.CameraEyeOffsetZ));
            prim.SetCameraAtOffset(new Vector3((float)primDoc.CameraAtOffsetX, (float)primDoc.CameraAtOffsetY, (float)primDoc.CameraAtOffsetZ));
            prim.SetForceMouselook(primDoc.ForceMouselook != 0);
            if(primDoc.sopanims != null)
                prim.DeSerializeAnimations(primDoc.sopanims);

            return prim;
        }

        private ShapeDocument ConvertToShapeDocument(PrimitiveBaseShape s, Guid primID)
        {
            return new ShapeDocument
            {
                UUID = primID,
                Shape = 0, // Not used
                ScaleX = s.Scale.X,
                ScaleY = s.Scale.Y,
                ScaleZ = s.Scale.Z,
                PCode = s.PCode,
                PathBegin = s.PathBegin,
                PathEnd = s.PathEnd,
                PathScaleX = s.PathScaleX,
                PathScaleY = s.PathScaleY,
                PathShearX = s.PathShearX,
                PathShearY = s.PathShearY,
                PathSkew = s.PathSkew,
                PathCurve = s.PathCurve,
                PathRadiusOffset = s.PathRadiusOffset,
                PathRevolutions = s.PathRevolutions,
                PathTaperX = s.PathTaperX,
                PathTaperY = s.PathTaperY,
                PathTwist = s.PathTwist,
                PathTwistBegin = s.PathTwistBegin,
                ProfileBegin = s.ProfileBegin,
                ProfileEnd = s.ProfileEnd,
                ProfileCurve = s.ProfileCurve,
                ProfileHollow = s.ProfileHollow,
                State = s.State,
                LastAttachPoint = s.LastAttachPoint,
                Texture = s.TextureEntry,
                ExtraParams = s.ExtraParams,
                Media = s.Media?.ToXml(),
                MatOvrd = s.RenderMaterialsOvrToRawBin()
            };
        }

        private PrimitiveBaseShape ConvertFromShapeDocument(ShapeDocument shapeDoc)
        {
            var s = new PrimitiveBaseShape
            {
                Scale = new Vector3((float)shapeDoc.ScaleX, (float)shapeDoc.ScaleY, (float)shapeDoc.ScaleZ),
                PCode = (byte)shapeDoc.PCode,
                PathBegin = (ushort)shapeDoc.PathBegin,
                PathEnd = (ushort)shapeDoc.PathEnd,
                PathScaleX = (byte)shapeDoc.PathScaleX,
                PathScaleY = (byte)shapeDoc.PathScaleY,
                PathShearX = (byte)shapeDoc.PathShearX,
                PathShearY = (byte)shapeDoc.PathShearY,
                PathSkew = (sbyte)shapeDoc.PathSkew,
                PathCurve = (byte)shapeDoc.PathCurve,
                PathRadiusOffset = (sbyte)shapeDoc.PathRadiusOffset,
                PathRevolutions = (byte)shapeDoc.PathRevolutions,
                PathTaperX = (sbyte)shapeDoc.PathTaperX,
                PathTaperY = (sbyte)shapeDoc.PathTaperY,
                PathTwist = (sbyte)shapeDoc.PathTwist,
                PathTwistBegin = (sbyte)shapeDoc.PathTwistBegin,
                ProfileBegin = (ushort)shapeDoc.ProfileBegin,
                ProfileEnd = (ushort)shapeDoc.ProfileEnd,
                ProfileCurve = (byte)shapeDoc.ProfileCurve,
                ProfileHollow = (ushort)shapeDoc.ProfileHollow,
                State = (byte)shapeDoc.State,
                LastAttachPoint = (byte)shapeDoc.LastAttachPoint,
                TextureEntry = shapeDoc.Texture,
                ExtraParams = shapeDoc.ExtraParams,
                Media = string.IsNullOrEmpty(shapeDoc.Media) ? null : PrimitiveBaseShape.MediaList.FromXml(shapeDoc.Media)
            };
            s.RenderMaterialsOvrFromRawBin(shapeDoc.MatOvrd);
            return s;
        }

        private ItemDocument ConvertToItemDocument(TaskInventoryItem item)
        {
            return new ItemDocument
            {
                itemID = item.ItemID,
                primID = item.ParentPartID,
                assetID = item.AssetID,
                parentFolderID = item.ParentID,
                invType = item.InvType,
                assetType = item.Type,
                name = item.Name,
                description = item.Description,
                creationDate = item.CreationDate,
                creatorID = item.CreatorIdentification,
                ownerID = item.OwnerID,
                lastOwnerID = item.LastOwnerID,
                groupID = item.GroupID,
                nextPermissions = item.NextPermissions,
                currentPermissions = item.CurrentPermissions,
                basePermissions = item.BasePermissions,
                everyonePermissions = item.EveryonePermissions,
                groupPermissions = item.GroupPermissions,
                flags = item.Flags
            };
        }

        private TaskInventoryItem ConvertFromItemDocument(ItemDocument doc)
        {
            return new TaskInventoryItem
            {
                ItemID = doc.itemID,
                ParentPartID = doc.primID,
                AssetID = doc.assetID,
                ParentID = doc.parentFolderID,
                InvType = doc.invType,
                Type = doc.assetType,
                Name = doc.name,
                Description = doc.description,
                CreationDate = (int)doc.creationDate,
                CreatorIdentification = doc.creatorID,
                OwnerID = doc.ownerID,
                LastOwnerID = doc.lastOwnerID,
                GroupID = doc.groupID,
                NextPermissions = doc.nextPermissions,
                CurrentPermissions = doc.currentPermissions,
                BasePermissions = doc.basePermissions,
                EveryonePermissions = doc.everyonePermissions,
                GroupPermissions = doc.groupPermissions,
                Flags = doc.flags
            };
        }

        private LandDocument ConvertToLandDocument(LandData land, UUID regionUUID)
        {
            return new LandDocument
            {
                UUID = land.GlobalID,
                RegionUUID = regionUUID,
                LocalLandID = land.LocalID,
                Bitmap = land.Bitmap,
                Name = land.Name,
                Desc = land.Description,
                OwnerUUID = land.OwnerID,
                IsGroupOwned = land.IsGroupOwned,
                Area = land.Area,
                AuctionID = land.AuctionID,
                Category = (int)land.Category,
                ClaimDate = land.ClaimDate,
                ClaimPrice = land.ClaimPrice,
                GroupUUID = land.GroupID,
                SalePrice = land.SalePrice,
                LandStatus = (int)land.Status,
                LandFlags = land.Flags,
                LandingType = land.LandingType,
                MediaAutoScale = land.MediaAutoScale,
                MediaTextureUUID = land.MediaID,
                MediaURL = land.MediaURL,
                MusicURL = land.MusicURL,
                PassHours = land.PassHours,
                PassPrice = (uint)land.PassPrice,
                SnapshotUUID = land.SnapshotID,
                UserLocationX = land.UserLocation.X,
                UserLocationY = land.UserLocation.Y,
                UserLocationZ = land.UserLocation.Z,
                UserLookAtX = land.UserLookAt.X,
                UserLookAtY = land.UserLookAt.Y,
                UserLookAtZ = land.UserLookAt.Z,
                AuthbuyerID = land.AuthBuyerID,
                OtherCleanTime = land.OtherCleanTime,
                Dwell = land.Dwell,
                MediaType = land.MediaType,
                MediaDescription = land.MediaDescription,
                MediaSize = $"{land.MediaWidth},{land.MediaHeight}",
                MediaLoop = land.MediaLoop,
                ObscureMedia = land.ObscureMedia,
                ObscureMusic = land.ObscureMusic,
                SeeAVs = land.SeeAVs,
                AnyAVSounds = land.AnyAVSounds,
                GroupAVSounds = land.GroupAVSounds,
                environment = land.Environment != null ? ViewerEnvironment.ToOSDString(land.Environment) : ""
            };
        }

        private LandData ConvertFromLandDocument(LandDocument doc)
        {
            var landData = new LandData
            {
                GlobalID = doc.UUID,
                LocalID = doc.LocalLandID,
                Bitmap = doc.Bitmap,
                Name = doc.Name,
                Description = doc.Desc,
                OwnerID = doc.OwnerUUID,
                IsGroupOwned = doc.IsGroupOwned,
                Area = doc.Area,
                AuctionID = (uint)doc.AuctionID,
                Category = (ParcelCategory)doc.Category,
                ClaimDate = doc.ClaimDate,
                ClaimPrice = doc.ClaimPrice,
                GroupID = doc.GroupUUID,
                SalePrice = doc.SalePrice,
                Status = (ParcelStatus)doc.LandStatus,
                Flags = doc.LandFlags,
                LandingType = doc.LandingType,
                MediaAutoScale = doc.MediaAutoScale,
                MediaID = doc.MediaTextureUUID,
                MediaURL = doc.MediaURL,
                MusicURL = doc.MusicURL,
                PassHours = doc.PassHours,
                PassPrice = (int)doc.PassPrice,
                SnapshotID = doc.SnapshotUUID,
                UserLocation = new Vector3((float)doc.UserLocationX, (float)doc.UserLocationY, (float)doc.UserLocationZ),
                UserLookAt = new Vector3((float)doc.UserLookAtX, (float)doc.UserLookAtY, (float)doc.UserLookAtZ),
                AuthBuyerID = doc.AuthbuyerID,
                OtherCleanTime = doc.OtherCleanTime,
                Dwell = doc.Dwell,
                MediaType = doc.MediaType,
                MediaDescription = doc.MediaDescription,
                MediaLoop = doc.MediaLoop,
                ObscureMedia = doc.ObscureMedia,
                ObscureMusic = doc.ObscureMusic,
                SeeAVs = doc.SeeAVs,
                AnyAVSounds = doc.AnyAVSounds,
                GroupAVSounds = doc.GroupAVSounds
            };

            if (!string.IsNullOrEmpty(doc.MediaSize))
            {
                string[] sizes = doc.MediaSize.Split(',');
                if (sizes.Length == 2)
                {
                    int.TryParse(sizes[0], out landData.MediaWidth);
                    int.TryParse(sizes[1], out landData.MediaHeight);
                }
            }
            
            if(!string.IsNullOrEmpty(doc.environment))
            {
                var venv = new ViewerEnvironment();
                venv.FromOSDString(doc.environment);
                landData.Environment = venv;
            }

            return landData;
        }

        private LandAccessDocument ConvertToLandAccessDocument(LandAccessEntry entry, UUID landID)
        {
            return new LandAccessDocument
            {
                LandUUID = landID,
                AccessUUID = entry.AgentID,
                Flags = (uint)entry.Flags
            };
        }

        private LandAccessEntry ConvertFromLandAccessDocument(LandAccessDocument doc)
        {
            return new LandAccessEntry
            {
                AgentID = doc.AccessUUID,
                Flags = (AccessList)doc.Flags,
                Expires = 0
            };
        }

        private RegionSettingsDocument ConvertToRegionSettingsDocument(RegionSettings settings)
        {
            return new RegionSettingsDocument
            {
                regionUUID = settings.RegionUUID,
                block_terraform = settings.BlockTerraform ? 1 : 0,
                block_fly = settings.BlockFly ? 1 : 0,
                allow_damage = settings.AllowDamage ? 1 : 0,
                restrict_pushing = settings.RestrictPushing ? 1 : 0,
                allow_land_resell = settings.AllowLandResell ? 1 : 0,
                allow_land_join_divide = settings.AllowLandJoinDivide ? 1 : 0,
                block_show_in_search = settings.BlockShowInSearch ? 1 : 0,
                agent_limit = settings.AgentLimit,
                object_bonus = settings.ObjectBonus,
                maturity = settings.Maturity,
                disable_scripts = settings.DisableScripts ? 1 : 0,
                disable_collisions = settings.DisableCollisions ? 1 : 0,
                disable_physics = settings.DisablePhysics ? 1 : 0,
                terrain_texture_1 = settings.TerrainTexture1.ToString(),
                terrain_texture_2 = settings.TerrainTexture2.ToString(),
                terrain_texture_3 = settings.TerrainTexture3.ToString(),
                terrain_texture_4 = settings.TerrainTexture4.ToString(),
                TerrainPBR1 = settings.TerrainPBR1.ToString(),
                TerrainPBR2 = settings.TerrainPBR2.ToString(),
                TerrainPBR3 = settings.TerrainPBR3.ToString(),
                TerrainPBR4 = settings.TerrainPBR4.ToString(),
                elevation_1_nw = settings.Elevation1NW,
                elevation_2_nw = settings.Elevation2NW,
                elevation_1_ne = settings.Elevation1NE,
                elevation_2_ne = settings.Elevation2NE,
                elevation_1_se = settings.Elevation1SE,
                elevation_2_se = settings.Elevation2SE,
                elevation_1_sw = settings.Elevation1SW,
                elevation_2_sw = settings.Elevation2SW,
                water_height = settings.WaterHeight,
                terrain_raise_limit = settings.TerrainRaiseLimit,
                terrain_lower_limit = settings.TerrainLowerLimit,
                use_estate_sun = settings.UseEstateSun ? 1 : 0,
                sandbox = settings.Sandbox ? 1 : 0,
                sunvectorx = settings.SunVector.X,
                sunvectory = settings.SunVector.Y,
                sunvectorz = settings.SunVector.Z,
                fixed_sun = settings.FixedSun ? 1 : 0,
                sun_position = settings.SunPosition,
                covenant = settings.Covenant.ToString(),
                covenant_datetime = settings.CovenantChangedDateTime,
                map_tile_ID = settings.TerrainImageID.ToString(),
                TelehubObject = settings.TelehubObject.ToString(),
                parcel_tile_ID = settings.ParcelImageID.ToString(),
                block_search = settings.GodBlockSearch,
                casino = settings.Casino,
                cacheID = settings.CacheID.ToString()
            };
        }

        private RegionSettings ConvertFromRegionSettingsDocument(RegionSettingsDocument doc)
        {
            var settings = new RegionSettings(doc.regionUUID)
            {
                BlockTerraform = doc.block_terraform != 0,
                BlockFly = doc.block_fly != 0,
                AllowDamage = doc.allow_damage != 0,
                RestrictPushing = doc.restrict_pushing != 0,
                AllowLandResell = doc.allow_land_resell != 0,
                AllowLandJoinDivide = doc.allow_land_join_divide != 0,
                BlockShowInSearch = doc.block_show_in_search != 0,
                AgentLimit = doc.agent_limit,
                ObjectBonus = doc.object_bonus,
                Maturity = doc.maturity,
                DisableScripts = doc.disable_scripts != 0,
                DisableCollisions = doc.disable_collisions != 0,
                DisablePhysics = doc.disable_physics != 0,
                TerrainTexture1 = new UUID(doc.terrain_texture_1),
                TerrainTexture2 = new UUID(doc.terrain_texture_2),
                TerrainTexture3 = new UUID(doc.terrain_texture_3),
                TerrainTexture4 = new UUID(doc.terrain_texture_4),
                TerrainPBR1 = new UUID(doc.TerrainPBR1),
                TerrainPBR2 = new UUID(doc.TerrainPBR2),
                TerrainPBR3 = new UUID(doc.TerrainPBR3),
                TerrainPBR4 = new UUID(doc.TerrainPBR4),
                Elevation1NW = doc.elevation_1_nw,
                Elevation2NW = doc.elevation_2_nw,
                Elevation1NE = doc.elevation_1_ne,
                Elevation2NE = doc.elevation_2_ne,
                Elevation1SE = doc.elevation_1_se,
                Elevation2SE = doc.elevation_2_se,
                Elevation1SW = doc.elevation_1_sw,
                Elevation2SW = doc.elevation_2_sw,
                WaterHeight = doc.water_height,
                TerrainRaiseLimit = doc.terrain_raise_limit,
                TerrainLowerLimit = doc.terrain_lower_limit,
                UseEstateSun = doc.use_estate_sun != 0,
                Sandbox = doc.sandbox != 0,
                SunVector = new Vector3((float)doc.sunvectorx, (float)doc.sunvectory, (float)doc.sunvectorz),
                FixedSun = doc.fixed_sun != 0,
                SunPosition = doc.sun_position,
                Covenant = new UUID(doc.covenant),
                CovenantChangedDateTime = doc.covenant_datetime,
                TerrainImageID = new UUID(doc.map_tile_ID),
                TelehubObject = new UUID(doc.TelehubObject),
                ParcelImageID = new UUID(doc.parcel_tile_ID),
                GodBlockSearch = doc.block_search,
                Casino = doc.casino,
                CacheID = new UUID(doc.cacheID)
            };
            return settings;
        }
    }
}