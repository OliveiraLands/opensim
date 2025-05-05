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
using OpenSim.Services.Interfaces;
using System.Threading;
using Timer = System.Timers.Timer;

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
        private string m_CloneAVATAR;
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
            m_CloneAVATAR = config.GetString("UUID_BotAvatarClone", "");

            //m_DiscordChannelId = (ulong)config.GetLong("DiscordChannelId", 0);
            string channelIdStr = config.GetString("DiscordChannelId", "0");
            if (ulong.TryParse(channelIdStr, out ulong channelId))
            {
                m_DiscordChannelId = channelId;
                m_log.Info($"[DiscordNPCBridge]: Channel ID set to {m_DiscordChannelId}");
            }
            else
            {
                m_log.Error($"[DiscordNPCBridge]: Failed to parse channel ID '{channelIdStr}' as ulong");
            }
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

            // CreateNPC(scene);

            //_ = SendDiscordMessage("[Teste] Bridge online!");

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
            InitializeDiscordAsync().GetAwaiter().GetResult();

            m_log.Info("[DiscordNPCBridge]: Post init. Creating NPC.");

            // Create the NPC in the first scene
            if (m_Scenes.Count > 0)
            {

                foreach (var scene in m_Scenes)
                {
                    CreateNPC(scene);
                }

                // Start periodic scan
                m_ScanTimer = new Timer(5000); // Scan every 5 seconds
                m_ScanTimer.Elapsed += OnScanTimerElapsed;
                m_ScanTimer.Start();
            }
            else
            {
                m_log.Warn("[DiscordNPCBridge]: No scenes found.");
            }
        }

        #endregion

        #region Discord Integration

        private string RecreateNPC()
        {
            string resultRecreate = "";
            // Create the NPC in the first scene
            if (m_Scenes.Count > 0)
            {
                foreach (var scene in m_Scenes)
                {
                    CreateNPC(scene);
                    resultRecreate = "NPC Created at scene " + scene.Name + "\n";
                }

                // Start periodic scan
                m_ScanTimer = new Timer(5000); // Scan every 5 seconds
                m_ScanTimer.Elapsed += OnScanTimerElapsed;
                m_ScanTimer.Start();
            }
            else
            {
                resultRecreate = "[DiscordNPCBridge]: No scenes found.";
                m_log.Warn(resultRecreate);

            }
            return resultRecreate;
        }
        public string RemoveNPCFromRegion()
        {
            string resultRemove = "";

            if (m_Scenes.Count > 0)
            {
                foreach (var scene in m_Scenes)
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
                }
            }
            return resultRemove;
        }

        private async Task InitializeDiscordAsync()
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
                    GatewayIntents = GatewayIntents.AllUnprivileged | GatewayIntents.MessageContent
                });

                m_DiscordClient.Log += LogDiscord;

                // Add a ready event to confirm when the bot is fully initialized
                TaskCompletionSource<bool> readyTask = new TaskCompletionSource<bool>();
                m_DiscordClient.Ready += () =>
                {
                    m_log.Info("[DiscordNPCBridge]: Discord client is fully ready");
                    readyTask.TrySetResult(true);
                    return Task.CompletedTask;
                };

                m_DiscordClient.MessageReceived += MessageReceived;

                await m_DiscordClient.LoginAsync(TokenType.Bot, m_DiscordToken);
                await m_DiscordClient.StartAsync();

                // Wait for ready event with timeout
                bool readySuccess = await Task.WhenAny(readyTask.Task, Task.Delay(30000)) == readyTask.Task;

                if (readySuccess)
                {
                    m_log.Info("[DiscordNPCBridge]: Discord connection fully initialized");

                    // Log available guilds and channels
                    foreach (var guild in m_DiscordClient.Guilds)
                    {
                        m_log.Info($"[DiscordNPCBridge]: Connected to guild: {guild.Name} ({guild.Id})");
                        foreach (var channel in guild.Channels)
                        {
                            m_log.Info($"[DiscordNPCBridge]: - Channel: {channel.Name} ({channel.Id})");
                        }
                    }

                    // Try to find our target channel
                    var targetChannel = m_DiscordClient.GetChannel(m_DiscordChannelId);
                    if (targetChannel != null)
                    {
                        m_log.Info($"[DiscordNPCBridge]: Target channel found: {targetChannel.GetType().Name}");
                    }
                    else
                    {
                        m_log.Error($"[DiscordNPCBridge]: Target channel {m_DiscordChannelId} NOT found");
                    }
                }
                else
                {
                    m_log.Error("[DiscordNPCBridge]: Timed out waiting for Discord client to be ready");
                }
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
            m_log.Info($"[DiscordNPCBridge]: Received message from {message.Author.Username} in channel {message.Channel.Id}");

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

            m_log.Info($"[DiscordNPCBridge]: Processing command: {command}");

            switch (command)
            {
                case "!help":
                    await SendDiscordMessage("Available commands:\n" +
                                           "!help - Display this help\n" +
                                           "!scan - Scan for nearby avatars\n" +
                                           "!walk x y z - Walk to the specified coordinates\n" +
                                           "!clone first last - clone avatar\n" +
                                           "!sit uuid - Sit on the specified object\n" +
                                           "!stand - Stand up\n" +
                                           "!ping - ping the NPC server\n" +
                                           "!recreate - recreate NPC at region\n" +
                                           "!remove - remove NPC from region\n" +
                                           "!status - Show bot status");
                    break;

                case "!recreate":
                    // Recreate NPC 
                    m_log.Info("[DiscordNPCBridge]: Executing recreate command");
                    string recreateResults = RecreateNPC();
                    await SendDiscordMessage(recreateResults);
                    break;

                case "!remove":
                    // Recreate NPC 
                    m_log.Info("[DiscordNPCBridge]: Executing remove command");
                    string removeResults = RemoveNPCFromRegion();
                    await SendDiscordMessage(removeResults);
                    break;

                case "!scan":
                    // Scan for nearby avatars and objects
                    m_log.Info("[DiscordNPCBridge]: Executing scan command");
                    string scanResults = ScanNearby();
                    await SendDiscordMessage(scanResults);
                    break;

                case "!clone":
                    m_log.Info("[DiscordNPCBridge]: Executing walk command");
                    if(parts .Length == 3)
                    {
                        CloneNPC(parts[1], parts[2]);
                        await SendDiscordMessage($"Cloned {parts[1]} {parts[2]}");
                    }
                    else
                    {
                        await SendDiscordMessage("Usage: !clone Firstname LastName");
                    }
                    break;

                case "!walk":
                    m_log.Info("[DiscordNPCBridge]: Executing walk command");
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
                    m_log.Info("[DiscordNPCBridge]: Executing sit command");
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
                    m_log.Info("[DiscordNPCBridge]: Executing stand command");
                    StandNPC();
                    await SendDiscordMessage("Standing up");
                    break;

                case "!status":
                    m_log.Info("[DiscordNPCBridge]: Executing Status command");
                    string status = GetNPCStatus();
                    await SendDiscordMessage(status);
                    break;
                case "!ping":
                    await SendDiscordMessage("Pong! Discord bridge is working.");
                    break;
                default:
                    await SendDiscordMessage($"Unknown command: {command}. Type !help for a list of commands.");
                    break;
            }
        }

        private async void CloneNPC(string v1, string v2)
        {
            if (!m_Enabled)
                return;

            string targetName = v1 + " " + v2;

            foreach (var kv in m_NPCScenes)
            {
                UUID oldNpcId = kv.Key;
                Scene scene = kv.Value;
                INPCModule npcMod = m_NPCModules[oldNpcId];

                // tenta achar o ScenePresence do avatar alvo
                var pres = scene.GetScenePresences()
                                .FirstOrDefault(p => p.Name.Equals(targetName, StringComparison.OrdinalIgnoreCase));
                if (pres != null)
                {
                    // clona a aparência
                    var avatarService = scene.RequestModuleInterface<IAvatarService>();
                    var appearance = avatarService?.GetAppearance(pres.UUID);
                    if (appearance == null)
                    {
                        await SendDiscordMessage($"Can't loca avatar appearence {targetName}.");
                        return;
                    }

                    // remove o NPC antigo
                    npcMod.DeleteNPC(oldNpcId, scene);
                    m_NPCModules.Remove(oldNpcId);
                    m_NPCScenes.Remove(oldNpcId);

                    // cria novo NPC com a mesma aparência
                    UUID newNpcId = npcMod.CreateNPC(
                        m_NPCFirstName,
                        m_NPCLastName,
                        m_NPCPosition,
                        scene.RegionInfo.EstateSettings.EstateOwner,
                        true,
                        scene,
                        appearance);

                    m_NPCModules[newNpcId] = npcMod;
                    m_NPCScenes[newNpcId] = scene;

                    await SendDiscordMessage($"NPC cloned with {targetName} (UUID {pres.UUID}).");
                    return;
                }
            }
        }

        private async Task SendDiscordMessage(string message)
        {
            try
            {
                m_log.Info($"[DiscordNPCBridge]: Attempting to send message to Discord. Channel ID: {m_DiscordChannelId}");
                var baseChannel = m_DiscordClient.GetChannel(m_DiscordChannelId);

                if (baseChannel == null)
                {
                    m_log.Error($"[DiscordNPCBridge]: Channel {m_DiscordChannelId} not found at all");

                    // Log all available channels for debugging
                    foreach (var guild in m_DiscordClient.Guilds)
                    {
                        m_log.Info($"[DiscordNPCBridge]: Bot is in guild: {guild.Name} ({guild.Id})");
                        foreach (var channel in guild.Channels)
                        {
                            m_log.Info($"[DiscordNPCBridge]: - Channel: {channel.Name} ({channel.Id})");
                        }
                    }
                    return;
                }

                if (baseChannel is not IMessageChannel textChannel)
                {
                    m_log.Error($"[DiscordNPCBridge]: Channel {m_DiscordChannelId} exists but is not a text channel");
                    return;
                }

                await textChannel.SendMessageAsync($" Region : {m_Scenes[0].Name} : {message}");
                m_log.Info("[DiscordNPCBridge]: Message sent successfully");
            }
            catch (Exception ex)
            {
                m_log.Error($"[DiscordNPCBridge]: Error in SendDiscordMessage: {ex.GetType().Name}: {ex.Message}");
                m_log.Error($"[DiscordNPCBridge]: Stack trace: {ex.StackTrace}");
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
                m_log.Info("[DiscordNPCBridge]: NPC module found. Creating NPC.");

                IAvatarService avatarService = scene.RequestModuleInterface<IAvatarService>();
                AvatarAppearance appearance = null;
                UUID ownerId = scene.RegionInfo.EstateSettings.EstateOwner;
                string origimAppearence = "nowhere";

                if (avatarService != null)
                {
                    if (m_CloneAVATAR != "")
                    {
                        try
                        {
                            appearance = avatarService.GetAppearance(UUID.Parse(m_CloneAVATAR));
                            origimAppearence = "cloned from .ini";
                        }
                        catch (Exception ex)
                        {
                            m_log.Error($"[DiscordNPCBridge]: Error getting appearance for UUID {m_CloneAVATAR}: {ex.Message}");
                        }
                    }
                    else
                    {
                        appearance = avatarService.GetAppearance(ownerId);
                        origimAppearence = "cloned from estate owner";
                    }

                    // Adiciona log para verificar a aparência recuperada
                    if (appearance != null)
                    {
                        m_log.Info($"[DiscordNPCBridge]: Appearance retrieved from {origimAppearence}. " +
                                   $"Wearables count: {appearance.Wearables.Length}, Attachments count: {appearance.GetAttachments().Count}");
                    }
                    else
                    {
                        m_log.Warn($"[DiscordNPCBridge]: Appearance is null for {origimAppearence}, using default appearance");
                        appearance = new AvatarAppearance();
                    }
                }
                else
                {
                    m_log.Warn("[DiscordNPCBridge]: AvatarService is null, using default appearance");
                    appearance = new AvatarAppearance();
                    origimAppearence = "nowhere (avatarService null)";
                }

                UUID ownerIdRoot = scene.RegionInfo.EstateSettings.EstateOwner;

                m_NPCUUID = npcModule.CreateNPC(m_NPCFirstName,
                                               m_NPCLastName,
                                               m_NPCPosition,
                                               ownerIdRoot,
                                               true,
                                               scene,
                                               appearance);

                m_log.Info($"[DiscordNPCBridge]: Created NPC {m_NPCFirstName} {m_NPCLastName} with UUID {m_NPCUUID} and appearance from {origimAppearence}");

                m_NPCModules[m_NPCUUID] = npcModule;
                m_NPCScenes[m_NPCUUID] = scene;

                SceneObjectGroup npcObj = scene.GetSceneObjectGroup(m_NPCUUID);
                if (npcObj != null)
                    m_log.Info($"[DiscordNPCBridge]: NPC AbsolutePosition = {npcObj.AbsolutePosition}");
                else
                    m_log.Warn("[DiscordNPCBridge]: Couldn't find SceneObjectGroup for NPC after creation!");

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
            m_log.Info("[DiscordNPCBridge]: Starting scan for nearby entities");

            if (m_NPCModules.Count == 0 || m_NPCScenes.Count == 0)
            {
                m_log.Warn("[DiscordNPCBridge]: No NPCs available for scanning");
                return "Error: No NPC available to perform scanning";
            }

            string result = "Nearby avatars and objects:\n";

            result += $"   Total NPC Modules found : {m_NPCModules.Count}\n";

            foreach (UUID npcId in m_NPCModules.Keys)
            {
                if (!m_NPCScenes.TryGetValue(npcId, out Scene scene))
                {
                    m_log.Warn($"[DiscordNPCBridge]: NPC {npcId} não associado a nenhuma cena");
                    continue;
                }

                // Quantos avatares/objetos a cena tem
                var presences = scene.GetScenePresences();
                var objects = scene.GetSceneObjectGroups();
                m_log.Info($"[DiscordNPCBridge]: Scene '{scene.RegionInfo.RegionName}' have {presences.Count} presences and {objects.Count} objects");

                SceneObjectGroup npcObject = scene.GetSceneObjectGroup(npcId);
                if (npcObject == null)
                {
                    m_log.Warn($"[DiscordNPCBridge]: I didn't find SceneObjectGroup for NPC {npcId}");
                    continue;
                }

                Vector3 npcPos = npcObject.AbsolutePosition;

                // Scan for avatars
                result += "   Avatars:\n";
                foreach (ScenePresence avatar in scene.GetScenePresences())
                {
                    if (avatar.UUID == npcId)
                        continue;

                    float dist = Vector3.Distance(npcPos, avatar.AbsolutePosition);
                    m_log.Info($"[DiscordNPCBridge]: found {avatar.Name} at {dist:F1}m");
                    if (dist <= m_ListenRadius * 2)
                        result += $"  {avatar.Name} ({dist:F1}m)\n";
                }

                // Scan for nearby objects
                result += "   Objects:\n";
                int objectCount = 0;
                foreach (SceneObjectGroup obj in scene.GetSceneObjectGroups())
                {
                    if (obj.UUID == npcId)
                        continue;

                    Vector3 objPos = obj.AbsolutePosition;
                    float distance = Vector3.Distance(npcPos, objPos);

                    if (distance <= m_ListenRadius * 2) // Double radius for scanning
                    {
                        result += $"     {obj.Name} (UUID: {obj.UUID}, {distance:F1}m away)\n";
                        objectCount++;

                        // Limit to 10 objects to avoid flooding
                        if (objectCount >= 10)
                        {
                            result += "     ... (and more)\n";
                            break;
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
                foreach (var scene in m_Scenes)
                {
                    CreateNPC(scene);
                }
            }
        }

        #endregion

        #region Chat Handling

        private void OnChatFromClient(Scene sender, OSChatMessage chat)
        {
            if (!m_Enabled)
                return;

            Scene scene = (Scene)sender;
            ScenePresence npcPresence = null;

            // Encontra o NPC na cena atual
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

            // Ignora mensagens enviadas pelo próprio NPC da cena
            if (chat.Sender.AgentId.Equals(npcPresence.UUID))
                return;

            // Obtém o remetente da mensagem
            ScenePresence senderPresence = scene.GetScenePresence(chat.Sender.AgentId);
            if (senderPresence == null)
                return;

            // Verifica se o remetente está dentro do raio de escuta
            if (Vector3.Distance(npcPresence.AbsolutePosition, senderPresence.AbsolutePosition) > m_ListenRadius)
                return;

            // Relaya apenas mensagens públicas, sussurros ou gritos
            if (chat.Type == ChatTypeEnum.Say || chat.Type == ChatTypeEnum.Whisper || chat.Type == ChatTypeEnum.Shout)
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

            // Only process object chat
             if (chat.Type != ChatTypeEnum.Say && chat.Type != ChatTypeEnum.Whisper && chat.Type != ChatTypeEnum.Shout)
                return;
            
            // m_log.Info($"[DiscordNPCBridge]: OnChatFromClient fired — Sender={chat.SenderUUID}, Type={chat.Type}, Message='{chat.Message}'");

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

            // Ignora mensagens enviadas pelo próprio NPC da cena
            if (chat.Sender.AgentId.Equals(npcPresence.UUID))
                return; ;

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
