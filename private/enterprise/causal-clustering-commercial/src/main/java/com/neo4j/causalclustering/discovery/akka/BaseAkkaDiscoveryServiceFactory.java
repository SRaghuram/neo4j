/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;

import java.time.Clock;
import java.util.concurrent.ExecutorService;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public abstract class BaseAkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
   protected abstract ActorSystemFactory actorSystemFactory( ExecutorService executor, Config config, LogProvider logProvider );

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver, RetryStrategy topologyServiceRetryStrategy,
            Monitors monitors, Clock clock )
    {
        ExecutorService executor = executorService( config, jobScheduler );

        return new AkkaCoreTopologyService(
                config,
                myself,
                actorSystemLifecycle( config, executor, logProvider, remoteMembersResolver ),
                logProvider,
                userLogProvider,
                topologyServiceRetryStrategy,
                executor,
                clock );
    }

    private ExecutorService executorService( Config config, JobScheduler jobScheduler )
    {
        int parallelism = config.get( CausalClusteringSettings.middleware_akka_default_parallelism_level );
        return jobScheduler.workStealingExecutorAsyncMode( Group.AKKA_TOPOLOGY_WORKER, parallelism );
    }

    @Override
    public TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            RemoteMembersResolver remoteMembersResolver, RetryStrategy topologyServiceRetryStrategy )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                myself,
                actorSystemLifecycle( config, executorService( config, jobScheduler ), logProvider, remoteMembersResolver )
        );
    }

    protected ActorSystemLifecycle actorSystemLifecycle( Config config, ExecutorService executor, LogProvider logProvider, RemoteMembersResolver resolver )
    {
        return new ActorSystemLifecycle(
                actorSystemFactory( executor, config, logProvider ),
                resolver,
                new JoinMessageFactory( resolver ),
                config,
                logProvider );
    }

}
