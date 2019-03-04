/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.remote.artery.tcp.SSLEngineProvider;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.AkkaDiscoverySSLEngineProvider;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

public class AkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    @Override
    public final CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver, RetryStrategy topologyServiceRetryStrategy,
            SslPolicyLoader sslPolicyLoader, Monitors monitors, Clock clock )
    {
        ExecutorService executor = executorService( config, jobScheduler );

        return new AkkaCoreTopologyService(
                config,
                myself,
                actorSystemLifecycle( config, executor, logProvider, remoteMembersResolver, sslPolicyLoader ),
                logProvider,
                userLogProvider,
                topologyServiceRetryStrategy,
                executor,
                clock );
    }

    @Override
    public final TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            RemoteMembersResolver remoteMembersResolver, RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                myself,
                actorSystemLifecycle( config, executorService( config, jobScheduler ), logProvider, remoteMembersResolver, sslPolicyLoader )
        );
    }

    protected ActorSystemLifecycle actorSystemLifecycle( Config config, ExecutorService executor, LogProvider logProvider, RemoteMembersResolver resolver,
            SslPolicyLoader sslPolicyLoader )
    {
        return new ActorSystemLifecycle(
                actorSystemFactory( sslPolicyLoader, executor, config, logProvider ),
                resolver,
                new JoinMessageFactory( resolver ),
                config,
                logProvider );
    }

    protected static ActorSystemFactory actorSystemFactory( SslPolicyLoader sslPolicyLoader, ExecutorService executor, Config config, LogProvider logProvider )
    {
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );
        Optional<SSLEngineProvider> sslEngineProvider = Optional.ofNullable( sslPolicy ).map( AkkaDiscoverySSLEngineProvider::new );
        return new ActorSystemFactory( sslEngineProvider, executor, config, logProvider );
    }

    private static ExecutorService executorService( Config config, JobScheduler jobScheduler )
    {
        int parallelism = config.get( CausalClusteringSettings.middleware_akka_default_parallelism_level );
        return jobScheduler.workStealingExecutorAsyncMode( Group.AKKA_TOPOLOGY_WORKER, parallelism );
    }
}
