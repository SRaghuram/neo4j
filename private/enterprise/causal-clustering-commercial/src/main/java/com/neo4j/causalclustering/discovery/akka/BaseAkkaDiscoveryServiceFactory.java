/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;

import java.time.Clock;

import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public abstract class BaseAkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
   protected abstract ActorSystemFactory actorSystemFactory( RemoteMembersResolver remoteMembersResolver, JobScheduler jobScheduler, Config config,
           LogProvider logProvider );

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy,
            Monitors monitors, Clock clock )
    {
        return new AkkaCoreTopologyService(
                config,
                myself,
                new ActorSystemLifecycle( actorSystemFactory( remoteMembersResolver, jobScheduler, config, logProvider ), logProvider ),
                logProvider,
                userLogProvider,
                topologyServiceRetryStrategy,
                clock );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            RemoteMembersResolver remoteMembersResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                myself,
                new ActorSystemLifecycle( actorSystemFactory( remoteMembersResolver, jobScheduler, config, logProvider ), logProvider ) );
    }

}
