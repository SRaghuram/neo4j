/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.time.Clock;

import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.HazelcastClient;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;

public class SslHazelcastDiscoveryServiceFactory extends HazelcastDiscoveryServiceFactory implements SslDiscoveryServiceFactory
{
    private SslPolicy sslPolicy;

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy, Monitors monitors, Clock clock )
    {
        configureHazelcast( config, logProvider );
        return new SslHazelcastCoreTopologyService( config, sslPolicy, myself, jobScheduler, logProvider,
                userLogProvider, remoteMembersResolver, topologyServiceRetryStrategy, monitors );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
            MemberId myself, RemoteMembersResolver remoteMembersResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config, logProvider );
        return new HazelcastClient( new SslHazelcastClientConnector( config, logProvider, sslPolicy, remoteMembersResolver ),
                jobScheduler, logProvider, config, myself );
    }

    @Override
    public void setSslPolicy( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }
}
