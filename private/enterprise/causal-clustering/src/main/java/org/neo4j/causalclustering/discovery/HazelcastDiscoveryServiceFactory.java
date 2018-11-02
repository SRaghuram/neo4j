/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.spi.properties.GroupProperty;

import java.util.logging.Level;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class HazelcastDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy, Monitors monitors )
    {
        configureHazelcast( config, logProvider );
        return new HazelcastCoreTopologyService( config, myself, jobScheduler, logProvider, userLogProvider, remoteMembersResolver,
                topologyServiceRetryStrategy, monitors );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider,
                                            JobScheduler jobScheduler, MemberId myself, RemoteMembersResolver remoteMembersResolver,
                                            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config, logProvider );
        return new HazelcastClient( new HazelcastClientConnector( config, logProvider, remoteMembersResolver ), jobScheduler,
                logProvider, config, myself );
    }

    protected static void configureHazelcast( Config config, LogProvider logProvider )
    {
        GroupProperty.WAIT_SECONDS_BEFORE_JOIN.setSystemProperty( "1" );
        GroupProperty.PHONE_HOME_ENABLED.setSystemProperty( "false" );
        GroupProperty.SOCKET_BIND_ANY.setSystemProperty( "false" );
        GroupProperty.SHUTDOWNHOOK_ENABLED.setSystemProperty( "false" );

        String licenseKey = config.get( CausalClusteringSettings.hazelcast_license_key );
        if ( licenseKey != null )
        {
            GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty( licenseKey );
        }

        // Make hazelcast quiet
        if ( config.get( CausalClusteringSettings.disable_middleware_logging ) )
        {
            // This is clunky, but the documented programmatic way doesn't seem to work
            GroupProperty.LOGGING_TYPE.setSystemProperty( "none" );
        }
        else
        {
            HazelcastLogging.enable( logProvider, new HazelcastLogLevel( config ) );
        }
    }

    private static class HazelcastLogLevel extends Level
    {
        HazelcastLogLevel( Config config )
        {
            super( "HAZELCAST", config.get( CausalClusteringSettings.middleware_logging_level ) );
        }
    }
}
