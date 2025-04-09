using System;
using System.Collections.Generic;
using Nini.Config;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Framework;
using OpenMetaverse;
using OpenSim.Services.Interfaces;
using OpenSim.Groups;

namespace OpenSim.Addons.AntiCopybot
{
    public class AntiCopybotModule : ISharedRegionModule
    {
        // Lista de regiões associadas ao módulo
        private List<Scene> m_scenes = new List<Scene>();
        // Registro de atividades por usuário
        private Dictionary<UUID, UserActivity> userActivities = new Dictionary<UUID, UserActivity>();
        // Limites para detecção de atividades suspeitas
        private const int MAX_ASSET_REQUESTS_PER_MINUTE = 100;
        private const int MAX_TELEPORTS_PER_MINUTE = 10;
        // UUID do grupo de administradores
        private UUID adminGroupID;
        private string adminUserRequest;
        /// <summary>
        /// Inicializa o módulo com configurações do arquivo .ini
        /// </summary>
        public void Initialise(IConfigSource source)
        {
            adminGroupID = new UUID(source.Configs["AntiCopybot"].GetString("AdminGroupID", UUID.Zero.ToString()));
            adminUserRequest = source.Configs["AntiCopybot"].GetString("AdminUserRequest", UUID.Zero.ToString());

            Console.WriteLine("[AntiCopybotModule] Módulo inicializado.");
        }

        /// <summary>
        /// Adiciona uma região ao módulo e registra eventos
        /// </summary>
        public void AddRegion(Scene scene)
        {
            m_scenes.Add(scene);
            scene.EventManager.OnClientLogin += OnClientLogin;
            scene.EventManager.OnClientClosed += OnClientLogout;
            //scene.EventManager OnAssetRequest += OnAssetRequest;
            //scene.EventManager.OnAvatarTeleport += OnAvatarTeleport;
        }

        private void OnClientLogout(UUID clientID, Scene scene)
        {
            throw new NotImplementedException();
        }

        private void OnClientLogin(IClientAPI aPI)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Remove uma região do módulo e cancela eventos
        /// </summary>
        public void RemoveRegion(Scene scene)
        {
            m_scenes.Remove(scene);
            scene.EventManager.OnClientLogin -= OnClientLogin;
            scene.EventManager.OnClientClosed -= OnClientLogout;
            //scene.EventManager.OnAssetRequest -= OnAssetRequest;
            //scene.EventManager.OnAvatarTeleport -= OnAvatarTeleport;
        }

        /// <summary>
        /// Executado após o carregamento de uma região
        /// </summary>
        public void RegionLoaded(Scene scene)
        {
            // Pode ser usado para inicializações adicionais, se necessário
        }

        /// <summary>
        /// Fecha o módulo
        /// </summary>
        public void Close()
        {
            // Não é necessário limpar eventos aqui, pois RemoveRegion já faz isso
            Console.WriteLine("[AntiCopybotModule] Módulo fechado.");
        }

        /// <summary>
        /// Nome do módulo
        /// </summary>
        public string Name => "AntiCopybotModule";

        /// <summary>
        /// Interface substituível (não aplicável neste caso)
        /// </summary>
        public Type ReplaceableInterface => null;

        // Evento: Cliente entra na região
        private void OnClientLogin(ScenePresence presence)
        {
            userActivities[presence.UUID] = new UserActivity();
        }

        // Evento: Cliente sai da região
        private void OnClientLogout(ScenePresence presence)
        {
            userActivities.Remove(presence.UUID);
        }

        // Evento: Requisição de ativo
        private void OnAssetRequest(UUID userID, UUID assetID)
        {
            if (userActivities.TryGetValue(userID, out UserActivity activity))
            {
                activity.AssetRequests++;
                if (activity.AssetRequests > MAX_ASSET_REQUESTS_PER_MINUTE)
                {
                    LogSuspiciousActivity(userID, "Requisições excessivas de ativos");
                }
            }
        }

        // Evento: Teleporte de avatar
        private void OnAvatarTeleport(ScenePresence presence, Vector3 position)
        {
            if (userActivities.TryGetValue(presence.UUID, out UserActivity activity))
            {
                activity.Teleports++;
                if (activity.Teleports > MAX_TELEPORTS_PER_MINUTE)
                {
                    LogSuspiciousActivity(presence.UUID, "Teleportes excessivos");
                }
            }
        }

        // Registra atividade suspeita e notifica administradores
        private void LogSuspiciousActivity(UUID userID, string reason)
        {
            Console.WriteLine($"[AntiCopybotModule] Suspeita de copybot: {userID} - {reason}");
            NotifyAdmins(userID, reason);
        }

        // Notifica os administradores do grupo configurado
        private void NotifyAdmins(UUID userID, string reason)
        {
            if (m_scenes.Count == 0) return;
            Scene scene = m_scenes[0]; // Usa a primeira região para acessar serviços
            GroupsService groupsService = scene.RequestModuleInterface<GroupsService>();
            if (groupsService == null) return;

            List<ExtendedGroupMembersData> adminMembers = groupsService.GetGroupMembers(adminUserRequest, adminGroupID);
            if (adminMembers == null || adminMembers.Count == 0) return;

            IPresenceService presenceService = scene.RequestModuleInterface<IPresenceService>();
            if (presenceService == null) return;

            /*
            foreach (ExtendedGroupMembersData adminID in adminMembers)
            {
                if (presenceService.IsUserOnline(adminID))
                {
                    string message = $"Suspeita de copybot: {userID} - {reason}";
                    scene.SimChat(message, ChatTypeEnum.Say, 0, adminID);
                }
            }
            */
        }

        public void PostInitialise()
        {
            throw new NotImplementedException();
        }

        // Classe interna para rastrear atividades de um usuário
        private class UserActivity
        {
            public int AssetRequests { get; set; }
            public int Teleports { get; set; }
        }
    }
}