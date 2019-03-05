/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

public class SharedDiscoveryServiceFactory implements DiscoveryServiceFactory
{

    private final SharedDiscoveryService discoveryService = new SharedDiscoveryService();

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
            RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader, Monitors monitors, Clock clock )
    {
        return new SharedDiscoveryCoreClient( discoveryService, myself, logProvider, config );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            RemoteMembersResolver remoteMembersResolver, RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader )
    {
        return new SharedDiscoveryReadReplicaClient( discoveryService, config, myself, logProvider );
    }

}
