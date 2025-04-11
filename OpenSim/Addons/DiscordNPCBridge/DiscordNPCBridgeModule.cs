using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using System.Timers;
using log4net;
using Mono.Addins;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using Discord;
using Discord.WebSocket;
using System.Linq;

namespace OpenSim.DiscordNPCBridge
{
    [Extension(Path = "/OpenSim/RegionModules", NodeName = "RegionModule", Id = "DiscordNPCBridgeModule")]
    public class DiscordNPCBridgeModule : ISharedRegionModule
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private bool m_Enabled = false;
        private List<Scene> m_Scenes = new List<Scene>();
        private IConfig m_Config;
        
        // Discord configuration
        private string m_DiscordToken = "";
        private ulong m_DiscordChannelId = 0;
        private DiscordSocketClient m_DiscordClient;
        
        // NPC configuration
        private UUID m_NPCUUID;
        private string m_NPCFirstName = "Discord";
        private string m_NPCLastName = "Bridge";
        private Vector3 m_NPCPosition = new Vector3(128, 128, 25);
        private float m_ListenRadius = 15.0f;
        private Dictionary<UUID, INPCModule> m_NPCModules = new Dictionary<UUID, INPCModule>();
        private Dictionary<UUID, Scene> m_NPCScenes = new Dictionary<UUID, Scene>();
        private Timer m_ScanTimer;

        private EventManager.ChatFromClientEvent m_OnChatFromClientHandler;


        #region ISharedRegionModule Interface

        public string Name => "DiscordNPCBridge";
        
        public Type ReplaceableInterface => null;
        
        public void Initialise(IConfigSource source)
        {
            IConfig config = source.Configs["DiscordNPCBridge"];
            if (config == null)
            {
                m_log.Info("[DiscordNPCBridge]: No configuration found, module disabled");
                return;
            }
            
            m_Config = config;
            m_Enabled = config.GetBoolean("Enabled", false);
            
            if (!m_Enabled)
                return;
                
            m_DiscordToken = config.GetString("DiscordToken", "");
            m_DiscordChannelId = (ulong)config.GetLong("DiscordChannelId", 0);
            m_NPCFirstName = config.GetString("NPCFirstName", "Discord");
            m_NPCLastName = config.GetString("NPCLastName", "Bridge");
            
            Vector3 pos = new Vector3();
            pos.X = config.GetFloat("NPCX", 128);
            pos.Y = config.GetFloat("NPCY", 128);
            pos.Z = config.GetFloat("NPCZ", 25);
            m_NPCPosition = pos;
            
            m_ListenRadius = config.GetFloat("ListenRadius", 15.0f);
            
            m_log.Info($"[DiscordNPCBridge]: Module initialized with channel {m_DiscordChannelId} and listen radius {m_ListenRadius}m");
        }
        
        public void AddRegion(Scene scene)
        {
            if (!m_Enabled)
                return;
                
            m_Scenes.Add(scene);
        }
        
        public void RegionLoaded(Scene scene)
        {
            if (!m_Enabled)
                return;
                
            m_log.Info($"[DiscordNPCBridge]: Region {scene.RegionInfo.RegionName} loaded");
            
            INPCModule npcModule = scene.RequestModuleInterface<INPCModule>();
            if (npcModule == null)
            {
                m_log.Error("[DiscordNPCBridge]: NPC module not found. Cannot continue.");
                return;
            }

            m_OnChatFromClientHandler = (s, chat) => OnChatFromClient(scene, chat);
            scene.EventManager.OnChatFromClient += m_OnChatFromClientHandler;

            scene.EventManager.OnChatFromWorld += OnChatFromWorld;
        }
        
        public void RemoveRegion(Scene scene)
        {
            if (!m_Enabled)
                return;
                
            if (m_Scenes.Contains(scene))
            {
                scene.EventManager.OnChatFromClient -= m_OnChatFromClientHandler;
                scene.EventManager.OnChatFromWorld -= OnChatFromWorld;
                
                if (m_NPCScenes.ContainsValue(scene))
                {
                    UUID npcId = m_NPCScenes.FirstOrDefault(x => x.Value == scene).Key;
                    if (npcId != UUID.Zero)
                    {
                        INPCModule npcModule = m_NPCModules[npcId];
                        npcModule.DeleteNPC(npcId, scene);
                        m_NPCModules.Remove(npcId);
                        m_NPCScenes.Remove(npcId);
                    }
                }
                
                m_Scenes.Remove(scene);
            }
        }
        
        public void Close()
        {
            if (!m_Enabled)
                return;
                
            ShutdownDiscord();
            
            if (m_ScanTimer != null)
            {
                m_ScanTimer.Stop();
                m_ScanTimer.Dispose();
                m_ScanTimer = null;
            }
            
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    INPCModule npcModule = m_NPCModules[npcId];
                    npcModule.DeleteNPC(npcId, scene);
                }
            }
            
            m_NPCModules.Clear();
            m_NPCScenes.Clear();
            m_Scenes.Clear();
        }
        
        public void PostInitialise()
        {
            if (!m_Enabled)
                return;
                
            // Initialize Discord connection
            InitializeDiscord();
            
            // Create the NPC in the first scene
            if (m_Scenes.Count > 0)
            {
                CreateNPC(m_Scenes[0]);
                
                // Start periodic scan
                m_ScanTimer = new Timer(5000); // Scan every 5 seconds
                m_ScanTimer.Elapsed += OnScanTimerElapsed;
                m_ScanTimer.Start();
            }
        }
        
        #endregion
        
        #region Discord Integration
        
        private async void InitializeDiscord()
        {
            if (string.IsNullOrEmpty(m_DiscordToken) || m_DiscordChannelId == 0)
            {
                m_log.Error("[DiscordNPCBridge]: Discord token or channel ID not configured properly.");
                return;
            }
            
            try
            {
                m_DiscordClient = new DiscordSocketClient(new DiscordSocketConfig
                {
                    GatewayIntents = GatewayIntents.Guilds | GatewayIntents.GuildMessages | GatewayIntents.MessageContent
                });
                
                m_DiscordClient.Log += LogDiscord;
                m_DiscordClient.MessageReceived += MessageReceived;
                
                await m_DiscordClient.LoginAsync(TokenType.Bot, m_DiscordToken);
                await m_DiscordClient.StartAsync();
                
                m_log.Info("[DiscordNPCBridge]: Discord connection initialized");
            }
            catch (Exception ex)
            {
                m_log.Error($"[DiscordNPCBridge]: Error initializing Discord connection: {ex.Message}");
            }
        }
        
        private Task LogDiscord(LogMessage msg)
        {
            m_log.Info($"[DiscordNPCBridge]: {msg.Message}");
            return Task.CompletedTask;
        }
        
        private async Task MessageReceived(SocketMessage message)
        {
            // Ignore messages from bots or from other channels
            if (message.Author.IsBot || message.Channel.Id != m_DiscordChannelId)
                return;
                
            string discordMessage = $"{message.Author.Username}: {message.Content}";
            m_log.Info($"[DiscordNPCBridge]: Discord message received: {discordMessage}");
            
            // Check for commands
            if (message.Content.StartsWith("!"))
            {
                await HandleDiscordCommand(message);
                return;
            }
            
            // Relay message to in-world through NPC
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    INPCModule npcModule = m_NPCModules[npcId];
                    npcModule.Say(npcId, scene, discordMessage);
                }
            }
        }
        
        private async Task HandleDiscordCommand(SocketMessage message)
        {
            string[] parts = message.Content.Split(' ');
            string command = parts[0].ToLower();
            
            switch (command)
            {
                case "!help":
                    await SendDiscordMessage("Available commands:\n" +
                                           "!help - Display this help\n" +
                                           "!scan - Scan for nearby avatars\n" +
                                           "!walk x y z - Walk to the specified coordinates\n" +
                                           "!sit uuid - Sit on the specified object\n" +
                                           "!stand - Stand up\n" +
                                           "!status - Show bot status");
                    break;
                    
                case "!scan":
                    // Scan for nearby avatars and objects
                    string scanResults = ScanNearby();
                    await SendDiscordMessage(scanResults);
                    break;
                    
                case "!walk":
                    if (parts.Length >= 4 && float.TryParse(parts[1], out float x) && 
                        float.TryParse(parts[2], out float y) && float.TryParse(parts[3], out float z))
                    {
                        WalkNPC(new Vector3(x, y, z));
                        await SendDiscordMessage($"Walking to position {x}, {y}, {z}");
                    }
                    else
                    {
                        await SendDiscordMessage("Usage: !walk x y z");
                    }
                    break;
                    
                case "!sit":
                    if (parts.Length >= 2 && UUID.TryParse(parts[1], out UUID targetId))
                    {
                        bool sat = SitNPC(targetId);
                        if (sat)
                            await SendDiscordMessage($"Sitting on object {targetId}");
                        else
                            await SendDiscordMessage($"Failed to sit on object {targetId}");
                    }
                    else
                    {
                        await SendDiscordMessage("Usage: !sit uuid");
                    }
                    break;
                    
                case "!stand":
                    StandNPC();
                    await SendDiscordMessage("Standing up");
                    break;
                    
                case "!status":
                    string status = GetNPCStatus();
                    await SendDiscordMessage(status);
                    break;
                    
                default:
                    await SendDiscordMessage($"Unknown command: {command}. Type !help for a list of commands.");
                    break;
            }
        }
        
        private async Task SendDiscordMessage(string message)
        {
            try
            {
                var channel = m_DiscordClient.GetChannel(m_DiscordChannelId) as IMessageChannel;
                if (channel != null)
                {
                    await channel.SendMessageAsync(message);
                }
            }
            catch (Exception ex)
            {
                m_log.Error($"[DiscordNPCBridge]: Error sending Discord message: {ex.Message}");
            }
        }
        
        private void ShutdownDiscord()
        {
            if (m_DiscordClient != null)
            {
                m_DiscordClient.StopAsync().Wait();
                m_DiscordClient.Dispose();
                m_DiscordClient = null;
            }
        }
        
        #endregion
        
        #region NPC Management
        
        private void CreateNPC(Scene scene)
        {
            INPCModule npcModule = scene.RequestModuleInterface<INPCModule>();
            if (npcModule == null)
            {
                m_log.Error("[DiscordNPCBridge]: NPC module not found. Cannot create NPC.");
                return;
            }
            
            try
            {
                AvatarAppearance avatarAppearance = new AvatarAppearance();

                m_NPCUUID = npcModule.CreateNPC(m_NPCFirstName, 
                                               m_NPCLastName, 
                                               m_NPCPosition,
                                               UUID.Zero, // Owner ID
                                               true,      // Set as AI
                                               scene, avatarAppearance);
                                               
                m_log.Info($"[DiscordNPCBridge]: Created NPC {m_NPCFirstName} {m_NPCLastName} with UUID {m_NPCUUID}");
                
                m_NPCModules[m_NPCUUID] = npcModule;
                m_NPCScenes[m_NPCUUID] = scene;

                // Say hello
                npcModule.Say(m_NPCUUID, scene, "Discord Bridge NPC activated. I'm relaying messages between OpenSim and Discord.");
            }
            catch (Exception ex)
            {
                m_log.Error($"[DiscordNPCBridge]: Error creating NPC: {ex.Message}");
            }
        }
        
        private void WalkNPC(Vector3 destination)
        {
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    INPCModule npcModule = m_NPCModules[npcId];
                    npcModule.MoveToTarget(npcId, scene, destination, false, true, false);
                }
            }
        }
        
        private bool SitNPC(UUID targetId)
        {
            bool success = false;
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    INPCModule npcModule = m_NPCModules[npcId];
                    success = npcModule.Sit(npcId, targetId, scene);
                }
            }
            return success;
        }
        
        private void StandNPC()
        {
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    INPCModule npcModule = m_NPCModules[npcId];
                    npcModule.Stand(npcId, scene);
                }
            }
        }
        
        private string ScanNearby()
        {
            string result = "Nearby avatars and objects:\n";
            
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    SceneObjectGroup npcObject = scene.GetSceneObjectGroup(npcId);
                    if (npcObject == null)
                        continue;
                        
                    Vector3 npcPos = npcObject.AbsolutePosition;
                    
                    // Scan for avatars
                    result += "Avatars:\n";
                    foreach (ScenePresence avatar in scene.GetScenePresences())
                    {
                        if (avatar.UUID == npcId)
                            continue;
                            
                        Vector3 avatarPos = avatar.AbsolutePosition;
                        float distance = Vector3.Distance(npcPos, avatarPos);
                        
                        if (distance <= m_ListenRadius * 2) // Double radius for scanning
                        {
                            result += $"  {avatar.Name} ({distance:F1}m away)\n";
                        }
                    }
                    
                    // Scan for nearby objects
                    result += "Objects:\n";
                    int objectCount = 0;
                    foreach (SceneObjectGroup obj in scene.GetSceneObjectGroups())
                    {
                        if (obj.UUID == npcId)
                            continue;
                            
                        Vector3 objPos = obj.AbsolutePosition;
                        float distance = Vector3.Distance(npcPos, objPos);
                        
                        if (distance <= m_ListenRadius * 2) // Double radius for scanning
                        {
                            result += $"  {obj.Name} (UUID: {obj.UUID}, {distance:F1}m away)\n";
                            objectCount++;
                            
                            // Limit to 10 objects to avoid flooding
                            if (objectCount >= 10)
                            {
                                result += "  ... (and more)\n";
                                break;
                            }
                        }
                    }
                }
            }
            
            return result;
        }
        
        private string GetNPCStatus()
        {
            string status = "Discord Bridge NPC Status:\n";
            
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    ScenePresence npcPresence = scene.GetScenePresence(npcId);
                    if (npcPresence != null)
                    {
                        status += $"Region: {scene.RegionInfo.RegionName}\n";
                        status += $"Position: {npcPresence.AbsolutePosition.X:F1}, {npcPresence.AbsolutePosition.Y:F1}, {npcPresence.AbsolutePosition.Z:F1}\n";
                        status += $"Listen radius: {m_ListenRadius}m\n";
                        status += $"Discord channel: {m_DiscordChannelId}\n";
                        status += $"Sitting: {npcPresence.IsSitting}\n";
                    }
                }
            }
            
            return status;
        }
        
        private void OnScanTimerElapsed(object sender, ElapsedEventArgs e)
        {
            // Periodically check if NPC still exists and recreate if needed
            bool needRecreate = true;
            
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    ScenePresence npcPresence = scene.GetScenePresence(npcId);
                    if (npcPresence != null)
                    {
                        needRecreate = false;
                        break;
                    }
                }
            }
            
            if (needRecreate && m_Scenes.Count > 0)
            {
                m_log.Info("[DiscordNPCBridge]: NPC not found, recreating...");
                CreateNPC(m_Scenes[0]);
            }
        }
        
        #endregion
        
        #region Chat Handling
        
        private void OnChatFromClient(Scene sender, OSChatMessage chat)
        {
            if (!m_Enabled)
                return;
                
            // Ignore chat from the NPC itself
            if (chat.Sender.Equals(m_NPCUUID))
                return;
                
            // Check if the chat source is within range of the NPC
            Scene scene = (Scene)sender;
            ScenePresence npcPresence = null;
            
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene npcScene) && npcScene == scene)
                {
                    npcPresence = scene.GetScenePresence(npcId);
                    break;
                }
            }
            
            if (npcPresence == null)
                return;
                
            // Get the chat senderPresence
            ScenePresence senderPresence = scene.GetScenePresence(chat.SenderUUID);
            if (senderPresence == null)
                return;
                
            // Check if within range
            if (Vector3.Distance(npcPresence.AbsolutePosition, senderPresence.AbsolutePosition) > m_ListenRadius)
                return;
                
            // Only relay public chat or whispers to the NPC
            if (chat.Type == ChatTypeEnum.Say || chat.Type == ChatTypeEnum.Whisper)
            {
                string message = $"{senderPresence.Name}: {chat.Message}";
                _ = SendDiscordMessage(message);
            }
        }
        
        private void OnChatFromWorld(Object sender, OSChatMessage chat)
        {
            // Similar to OnChatFromClient but for objects
            if (!m_Enabled)
                return;
                
            // Ignore chat from the NPC itself
            if (chat.Sender.Equals(m_NPCUUID))
                return;
                
            // Only process object chat
            if (chat.Type != ChatTypeEnum.Say && chat.Type != ChatTypeEnum.Whisper)
                return;
                
            Scene scene = (Scene)sender;
            ScenePresence npcPresence = null;
            
            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (m_NPCScenes.TryGetValue(npcId, out Scene npcScene) && npcScene == scene)
                {
                    npcPresence = scene.GetScenePresence(npcId);
                    break;
                }
            }
            
            if (npcPresence == null)
                return;
                
            // Get the object position
            SceneObjectPart part = scene.GetSceneObjectPart(chat.SenderUUID);
            if (part == null)
                return;
                
            // Check if within range
            if (Vector3.Distance(npcPresence.AbsolutePosition, part.AbsolutePosition) > m_ListenRadius)
                return;
                
            string message = $"[Object {part.Name}]: {chat.Message}";
            _ = SendDiscordMessage(message);
        }
        
        #endregion
    }
}
