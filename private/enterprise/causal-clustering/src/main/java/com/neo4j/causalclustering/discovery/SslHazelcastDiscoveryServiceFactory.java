/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

public class SslHazelcastDiscoveryServiceFactory extends HazelcastDiscoveryServiceFactory
{
    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
            RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader, Monitors monitors, Clock clock )
    {
        configureHazelcast( config, logProvider );
        SslPolicy sslPolicy = createSslPolicy( sslPolicyLoader, config );
        return new SslHazelcastCoreTopologyService( config, sslPolicy, myself, jobScheduler, logProvider,
                userLogProvider, remoteMembersResolver, topologyServiceRetryStrategy, monitors );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
            MemberId myself, RemoteMembersResolver remoteMembersResolver,
            RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader )
    {
        configureHazelcast( config, logProvider );
        SslPolicy sslPolicy = createSslPolicy( sslPolicyLoader, config );
        return new HazelcastClient( new SslHazelcastClientConnector( config, logProvider, sslPolicy, remoteMembersResolver ),
                jobScheduler, logProvider, config, myself );
    }

    private static SslPolicy createSslPolicy( SslPolicyLoader sslPolicyLoader, Config config )
    {
        return sslPolicyLoader.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );
    }
}
