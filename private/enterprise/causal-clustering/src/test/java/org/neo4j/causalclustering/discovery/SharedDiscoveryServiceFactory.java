/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class SharedDiscoveryServiceFactory implements DiscoveryServiceFactory
{

    private final SharedDiscoveryService discoveryService = new SharedDiscoveryService();

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy, Monitors monitors )
    {
        return new SharedDiscoveryCoreClient( discoveryService, myself, logProvider, config );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            RemoteMembersResolver remoteMembersResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        return new SharedDiscoveryReadReplicaClient( discoveryService, config, myself, logProvider );
    }

}
