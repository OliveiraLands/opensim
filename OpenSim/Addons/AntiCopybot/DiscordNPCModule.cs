using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Nini.Config;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenMetaverse;
using OpenSim.Framework;

namespace OpenSim.Region.OptionalModules.DiscordBridge
{
    public class DiscordNPCModule : IRegionModuleBase
    {
        private Scene m_scene; // A cena (região) onde o NPC será criado
        private UUID m_npcId; // ID do NPC criado
        private string m_discordWebhookUrl; // URL do webhook do Discord
        private HttpClient m_httpClient = new HttpClient(); // Cliente HTTP para enviar mensagens ao Discord

        public string Name => "DiscordNPCModule";
        public Type ReplaceableInterface => null;

        public void Initialise(IConfigSource source)
        {
            // Lê a URL do webhook do Discord a partir do arquivo de configuração (OpenSim.ini)
            m_discordWebhookUrl = source.Configs["DiscordNPC"].GetString("WebhookUrl", string.Empty);
            if (string.IsNullOrEmpty(m_discordWebhookUrl))
            {
                Console.WriteLine("[DiscordNPCModule] Webhook URL não configurada. Módulo desativado.");
            }
        }

        public void AddRegion(Scene scene)
        {
            m_scene = scene;
            m_scene.EventManager.OnNewClient += OnNewClient; // Evento para detectar novos clientes
            CreateNPC(); // Cria o NPC ao adicionar a região
        }

        public void RemoveRegion(Scene scene)
        {
            if (m_npcId != UUID.Zero)
            {
                m_scene.GetModule<INPCModule>()?.RemoveNPC(m_npcId, m_scene);
                SendDiscordMessage("NPC removido da região.");
            }
        }

        public void RegionLoaded(Scene scene)
        {
            // Nada a fazer aqui por enquanto
        }

        public void Close()
        {
            m_httpClient.Dispose();
        }

        // Cria o NPC na região
        private void CreateNPC()
        {
            INPCModule npcModule = m_scene.GetModule<INPCModule>();
            if (npcModule != null)
            {
                Vector3 startPos = new Vector3(128, 128, 25); // Posição inicial do NPC (centro da região)
                m_npcId = npcModule.CreateNPC("DiscordBot", "NPC", startPos, m_scene, false);
                SendDiscordMessage("NPC DiscordBot criado na região!");
            }
        }

        // Evento disparado quando um novo cliente entra na região
        private void OnNewClient(IClientAPI client)
        {
            client.OnChatFromClient += OnChatFromClient; // Escuta mensagens de chat
            SendDiscordMessage($"Novo avatar entrou na região: {client.Name}");
        }

        // Processa mensagens de chat na região
        private void OnChatFromClient(object sender, OSChatMessage chat)
        {
            string message = chat.Message;
            if (message.StartsWith("/discord "))
            {
                string discordMessage = message.Substring(9); // Remove o prefixo "/discord "
                SendDiscordMessage($"Mensagem de {chat.From}: {discordMessage}");

                // Faz o NPC responder no mundo virtual
                INPCModule npcModule = m_scene.GetModule<INPCModule>();
                if (npcModule != null && m_npcId != UUID.Zero)
                {
                    npcModule.Say(m_npcId, m_scene, $"Recebi sua mensagem: {discordMessage}");
                }
            }
        }

        // Envia uma mensagem para o canal do Discord via webhook
        private async void SendDiscordMessage(string message)
        {
            if (string.IsNullOrEmpty(m_discordWebhookUrl)) return;

            var payload = new
            {
                content = message,
                username = "OpenSimNPC" // Nome do bot no Discord
            };

            try
            {
                string jsonPayload = JsonConvert.SerializeObject(payload);
                var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                await m_httpClient.PostAsync(m_discordWebhookUrl, content);
            }
            catch (Exception e)
            {
                Console.WriteLine($"[DiscordNPCModule] Erro ao enviar mensagem para o Discord: {e.Message}");
            }
        }
    }
}