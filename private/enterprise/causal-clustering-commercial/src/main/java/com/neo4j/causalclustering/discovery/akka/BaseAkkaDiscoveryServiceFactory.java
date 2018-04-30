/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.HostnameResolver;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public abstract class BaseAkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
   protected abstract ActorSystemFactory actorSystemFactory( Config config, LogProvider logProvider );

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, HostnameResolver hostnameResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy, Monitors monitors )
    {
        return new AkkaCoreTopologyService(
                config,
                myself,
                new ActorSystemLifecycle( actorSystemFactory( config, logProvider ), logProvider ),
                jobScheduler,
                logProvider,
                userLogProvider,
                hostnameResolver,
                topologyServiceRetryStrategy
        );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            HostnameResolver hostnameResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                jobScheduler,
                myself,
                hostnameResolver,
                topologyServiceRetryStrategy,
                new ActorSystemLifecycle( actorSystemFactory( config, logProvider ), logProvider )
        );
    }

}
