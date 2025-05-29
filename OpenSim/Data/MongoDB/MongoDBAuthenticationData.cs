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

using log4net;
using OpenMetaverse;
using OpenSim.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;

namespace OpenSim.Data.MongoDB
{
    public class MongoDBAuthenticationData : MongoDBFramework, IAuthenticationData
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected MongoDBGenericTableHandler<AuthenticationData> m_authdata;

        private string m_Realm;
        private List<string> m_ColumnNames;
        private int m_LastExpire;

        private static bool m_initialized = false;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MongoDBAuthenticationData(string connectionString, string realm)
                : base(connectionString)
        {
            m_Realm = realm;

            m_authdata = new MongoDBGenericTableHandler<AuthenticationData>(connectionString, realm, realm, "PrincipalID");

        }

        public AuthenticationData Get(UUID principalID)
        {
            AuthenticationData ret = m_authdata.Get("PrincipalID", principalID.ToString())?.First();

            return ret ?? null;
        }

        public bool Store(AuthenticationData data)
        {
            return m_authdata.Store(data);
        }

        public bool SetDataItem(UUID principalID, string item, string value)
        {
            using (MongoDBCommand cmd = new MongoDBCommand("update `" + m_Realm +
                    "` set `" + item + "` = " + value + " where UUID = '" + principalID.ToString() + "'"))
            {
                if (ExecuteNonQuery(cmd, m_Connection) > 0)
                    return true;
            }

            return false;
        }

        public bool SetToken(UUID principalID, string token, int lifetime)
        {
            if (System.Environment.TickCount - m_LastExpire > 30000)
                DoExpire();

            using (MongoDBCommand cmd = new MongoDBCommand("insert into tokens (UUID, token, validity) values ('" + principalID.ToString() +
                "', '" + token + "', datetime('now', 'localtime', '+" + lifetime.ToString() + " minutes'))"))
            {
                if (ExecuteNonQuery(cmd, m_Connection) > 0)
                    return true;
            }

            return false;
        }

        public bool CheckToken(UUID principalID, string token, int lifetime)
        {
            if (System.Environment.TickCount - m_LastExpire > 30000)
                DoExpire();

            using (MongoDBCommand cmd = new MongoDBCommand("update tokens set validity = datetime('now', 'localtime', '+" + lifetime.ToString() +
                " minutes') where UUID = '" + principalID.ToString() + "' and token = '" + token + "' and validity > datetime('now', 'localtime')"))
            {
                if (ExecuteNonQuery(cmd, m_Connection) > 0)
                    return true;
            }

            return false;
        }

        private void DoExpire()
        {
            using (MongoDBCommand cmd = new MongoDBCommand("delete from tokens where validity < datetime('now', 'localtime')"))
                ExecuteNonQuery(cmd, m_Connection);

            m_LastExpire = System.Environment.TickCount;
        }
    }
}