/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.time.Clock;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public interface DiscoveryServiceFactory
{
    CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider, LogProvider userLogProvider,
            RemoteMembersResolver remoteMembersResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy, Monitors monitors, Clock clock );

    TopologyService readReplicaTopologyService( Config config, LogProvider logProvider,
            JobScheduler jobScheduler, MemberId myself, RemoteMembersResolver remoteMembersResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy );
}
