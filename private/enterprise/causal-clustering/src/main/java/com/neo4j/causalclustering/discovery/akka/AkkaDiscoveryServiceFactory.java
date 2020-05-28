/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.remote.artery.tcp.SSLEngineProvider;
import com.neo4j.causalclustering.discovery.AkkaDiscoverySSLEngineProvider;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

public class AkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    private static final long RESTART_RETRY_DELAY_MS = 1000L;
    private static final long RESTART_RETRY_DELAY_MAX_MS = 60 * 1000L;
    private static final int RESTART_FAILURES_BEFORE_UNHEALTHY = 8;

    @Override
    public final AkkaCoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver, RetryStrategy catchupAddressRetryStrategy,
            SslPolicyLoader sslPolicyLoader, DiscoveryMemberFactory discoveryMemberFactory, Monitors monitors, Clock clock )
    {
        Executor executor = executorService( config, jobScheduler );
        TimeoutStrategy timeoutStrategy = new ExponentialBackoffStrategy( RESTART_RETRY_DELAY_MS, RESTART_RETRY_DELAY_MAX_MS, MILLISECONDS );
        Restarter restarter = new Restarter( timeoutStrategy, RESTART_FAILURES_BEFORE_UNHEALTHY );

        return new AkkaCoreTopologyService(
                config,
                myself,
                actorSystemLifecycle( config, executor, logProvider, remoteMembersResolver, sslPolicyLoader ),
                logProvider,
                userLogProvider,
                catchupAddressRetryStrategy,
                restarter,
                discoveryMemberFactory,
                executor,
                clock,
                monitors );
    }

    @Override
    public final AkkaTopologyClient readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader, DiscoveryMemberFactory discoveryMemberFactory, Clock clock )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                myself,
                actorSystemLifecycle( config, executorService( config, jobScheduler ), logProvider, remoteMembersResolver, sslPolicyLoader ),
                discoveryMemberFactory,
                clock );
    }

    protected ActorSystemLifecycle actorSystemLifecycle( Config config, Executor executor, LogProvider logProvider, RemoteMembersResolver resolver,
            SslPolicyLoader sslPolicyLoader )
    {
        return new ActorSystemLifecycle(
                actorSystemFactory( sslPolicyLoader, executor, config, logProvider ),
                resolver,
                new JoinMessageFactory( resolver ),
                config,
                logProvider );
    }

    protected static ActorSystemFactory actorSystemFactory( SslPolicyLoader sslPolicyLoader, Executor executor, Config config, LogProvider logProvider )
    {

        SslPolicy sslPolicy = sslPolicyLoader.hasPolicyForSource( CLUSTER ) ? sslPolicyLoader.getPolicy( CLUSTER ) : null;
        Optional<SSLEngineProvider> sslEngineProvider = Optional.ofNullable( sslPolicy ).map( AkkaDiscoverySSLEngineProvider::new );
        return new ActorSystemFactory( sslEngineProvider, executor, config, logProvider );
    }

    private static Executor executorService( Config config, JobScheduler jobScheduler )
    {
        int parallelism = config.get( CausalClusteringInternalSettings.middleware_akka_default_parallelism_level );
        jobScheduler.setParallelism( Group.AKKA_TOPOLOGY_WORKER, parallelism );
        return jobScheduler.executor( Group.AKKA_TOPOLOGY_WORKER );
    }
}
