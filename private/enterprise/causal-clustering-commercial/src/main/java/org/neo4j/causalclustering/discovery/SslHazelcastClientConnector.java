/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.client.config.ClientNetworkConfig;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

import static org.neo4j.causalclustering.discovery.HazelcastSslConfiguration.configureSsl;

public class SslHazelcastClientConnector extends HazelcastClientConnector
{
    private final SslPolicy sslPolicy;

    SslHazelcastClientConnector( Config config, LogProvider logProvider, SslPolicy sslPolicy,
                                 HostnameResolver hostnameResolver )
    {
        super( config, logProvider, hostnameResolver );
        this.sslPolicy = sslPolicy;
    }

    @Override
    protected void additionalConfig( ClientNetworkConfig networkConfig, LogProvider logProvider )
    {
        configureSsl( networkConfig, sslPolicy, logProvider );
    }
}
