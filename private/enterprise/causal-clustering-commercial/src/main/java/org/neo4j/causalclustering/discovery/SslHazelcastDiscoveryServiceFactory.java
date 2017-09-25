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

import com.hazelcast.spi.properties.GroupProperty;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;

public class SslHazelcastDiscoveryServiceFactory extends HazelcastDiscoveryServiceFactory
{
    private SslPolicy sslPolicy;

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config );
        return new SslHazelcastCoreTopologyService( config, sslPolicy, myself, jobScheduler, logProvider,
                userLogProvider, hostnameResolver, topologyServiceRetryStrategy );
    }

    @Override
    public TopologyService topologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
                                            MemberId myself, HostnameResolver hostnameResolver,
                                            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config );
        return new HazelcastClient( new SslHazelcastClientConnector( config, logProvider, sslPolicy, hostnameResolver ),
                jobScheduler, logProvider, config, myself, topologyServiceRetryStrategy );
    }

    private static void configureHazelcast( Config config )
    {
        // tell hazelcast to not phone home
        System.setProperty( "hazelcast.phone.home.enabled", "false" );
        System.setProperty( "hazelcast.socket.server.bind.any", "false" );

        String licenseKey = config.get( CausalClusteringSettings.hazelcast_license_key );
        if ( licenseKey != null )
        {
            GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty( licenseKey );
        }

        // Make hazelcast quiet
        if ( config.get( CausalClusteringSettings.disable_middleware_logging ) )
        {
            // This is clunky, but the documented programmatic way doesn't seem to work
            System.setProperty( "hazelcast.logging.type", "none" );
        }
    }

    public void setSslPolicy( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }
}
