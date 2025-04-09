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

namespace OpenSim.OLRegionMods
{
    [Extension(Path = "/OpenSim/RegionModules", NodeName = "RegionModule", Id = "ChatLogModule")]
    public class ChatLogModule : ISharedRegionModule
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private bool m_Enabled = false;
        private List<Scene> m_SceneList = new List<Scene>();

        private StreamWriter logFile;

        public void Initialise(IConfigSource config)
        {
            //IConfig cnf = config.Configs["ChatLog"];
            //if (cnf == null)
            //    return;
            //if (cnf != null && cnf.GetString("LogFile", string.Empty) != Name)
            //    return;

            m_Enabled = true;

            m_log.DebugFormat("[ChatLog]: Log Chat log enabled by {0}", Name);

            logFile = new StreamWriter("chatlog.log", true);
        }

        public void AddRegion(Scene scene)
        {
            if (!m_Enabled)
                return;

            //scene.RegisterModuleInterface<IOfflineIMService>(this);
            m_SceneList.Add(scene);
            scene.EventManager.OnNewClient += OnNewClient;
        }

        public void RegionLoaded(Scene scene)
        {
            if (!m_Enabled)
                return;
        }

        public void RemoveRegion(Scene scene)
        {
            if (!m_Enabled)
                return;

            m_SceneList.Remove(scene);
            scene.EventManager.OnNewClient -= OnNewClient;

            scene.ForEachClient(delegate(IClientAPI client)
            {
                //client.OnRetrieveInstantMessages -= RetrieveInstantMessages;
            });
        }

        public void PostInitialise()
        {
        }

        public string Name
        {
            get { return "Chat Log Module V1"; }
        }

        public Type ReplaceableInterface
        {
            get { return null; }
        }

        public void Close()
        {
            m_SceneList.Clear();
        }

        private Scene FindScene(UUID agentID)
        {
            foreach (Scene s in m_SceneList)
            {
                ScenePresence presence = s.GetScenePresence(agentID);
                if (presence != null && !presence.IsChildAgent)
                    return s;
            }
            return null;
        }

        private IClientAPI FindClient(UUID agentID)
        {
            foreach (Scene s in m_SceneList)
            {
                ScenePresence presence = s.GetScenePresence(agentID);
                if (presence != null && !presence.IsChildAgent)
                    return presence.ControllingClient;
            }
            return null;
        }

        private void OnNewClient(IClientAPI client)
        {
            // client.OnRetrieveInstantMessages += RetrieveInstantMessages;
            client.OnChatFromClient += Client_OnChatFromClient;
        }

        private void Client_OnChatFromClient(object sender, OSChatMessage e)
        {
            if (e.Channel == 0)
            {
                logFile.WriteLine("[{0}] {1}: {2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), e.Sender.Name, e.Message);
                logFile.Flush();
            }
        }


    }
}

