/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

public interface DiscoveryServiceFactory
{
    CoreTopologyService coreTopologyService( Config config, ClusteringIdentityModule identityModule, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver, RetryStrategy topologyServiceRetryStrategy,
            SslPolicyLoader sslPolicyLoader, DiscoveryMemberFactory discoveryMemberFactory, Monitors monitors, Clock clock );

    TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, ClusteringIdentityModule identityModule,
            RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader, DiscoveryMemberFactory discoveryMemberFactory, Clock clock );
}
