using System;
using System.Collections.Generic;
using System.Reflection;
using log4net;
using Mono.Addins;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Framework.Servers;
using OpenSim.Framework.Client;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Services.Interfaces;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using OpenSim.Region.CoreModules.Avatar.Chat;

namespace OpenSim.OLRegionMods
{
    public class NPCAgentModule : ISharedRegionModule
    {
        private Scene m_scene;
        private ChatModule m_chatModule;
        private IAgentInfoService m_agentInfoService;
        private UUID m_npcAgentId;
        private Random m_random = new Random();
        private string m_chatgptApiUrl;
        private string m_chatgptApiKey;

        public void Initialize(Scene scene)
        {
            m_scene = scene;
            //m_chatModule = m_scene.RequestModuleInterface<ChatModule>();
            m_agentInfoService = m_scene.RequestModuleInterface<IAgentInfoService>();

            m_chatgptApiUrl = "https://api.openai.com/v1/engines/davinci/completions";
            m_chatgptApiKey = "YOUR_API_KEY";
            m_npcAgentId = UUID.Random();

            
            // Create the NPC agent
            CreateNPCAgent();
            m_chatModule.OnChatFromWorld += OnChat;
        }

        public void Close() { }
        public string Name { get { return "NPCAgentModule"; } }
        public bool IsSharedModule { get { return false; } }

        string IRegionModuleBase.Name => throw new NotImplementedException();

        Type IRegionModuleBase.ReplaceableInterface => throw new NotImplementedException();

        private void CreateNPCAgent()
        {
            IAgentInfo agent = new IAgentInfo();
            agent.AgentOnline = true;
            agent.AgentID = m_npcAgentId;
            agent.IsChildAgent = false;
            agent.Name = "NPC";
            agent.RegionID = m_scene.RegionInfo.RegionID;
            agent.ServiceURLs = new Dictionary<string, object>();
            m_agentInfoService.SetRootAgent(agent);
        }

        private void OnChat(string message, ChatTypeEnum type, int channel, Vector3 fromPos, string fromName, UUID fromID, 
            ChatSourceTypeEnum sourceType, bool fromAgent, WorldObject obj)
        {
            // Only respond to NPC if the message is not from the NPC itself
            if (fromID != m_npcAgentId && type == ChatTypeEnum.Say)
            {
                // Check if the message is a question
                if (message.EndsWith("?"))
                {
                    // Randomly decide if the NPC will answer the question
                    if (m_random.Next(100) < 10)
                    {
                        // Send the question to the GPT-3 API
                        HttpWebRequest request = (HttpWebRequest)WebRequest.Create(m_chatgptApiUrl);
                        request.Method = "POST";
                        request.ContentType = "application/json";
                        request.Headers.Add("Authorization", "Bearer " + m_chatgptApiKey);
                        string postData = JsonConvert.SerializeObject(new
                        {
                            prompt = "NPC: " + message,
                            temperature = 0.5f,
                            max_tokens = 50
                        });
                        byte[] data = System.Text.Encoding.ASCII.GetBytes(postData);
                        request.ContentLength = data.Length;
                        using (Stream stream = request.GetRequestStream())
                        {
                            stream.Write(data, 0, data.Length);
                        }
                        HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                        // Get the response from the GPT-3 API
                        string responseText;
                        using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                        {
                            responseText = sr.ReadToEnd();
                        }
                        var result = JsonConvert.DeserializeObject<dynamic>(responseText);

                        // Send the answer from the GPT-3 API to the chat
                        string answer = result.choices[0].text;
                        m_chatModule.SimChat("NPC: " + answer, ChatTypeEnum.Say, 0, fromPos, "NPC", m_npcAgentId, ChatSourceType.Object, false);
                    }
                }
            }
        }

        void IRegionModuleBase.Initialise(global::Nini.Config.IConfigSource source)
        {
            throw new NotImplementedException();
        }

        void IRegionModuleBase.Close()
        {
            throw new NotImplementedException();
        }

        void IRegionModuleBase.AddRegion(Scene scene)
        {
            throw new NotImplementedException();
        }

        void IRegionModuleBase.RemoveRegion(Scene scene)
        {
            throw new NotImplementedException();
        }

        void IRegionModuleBase.RegionLoaded(Scene scene)
        {
            throw new NotImplementedException();
        }
    }
}
