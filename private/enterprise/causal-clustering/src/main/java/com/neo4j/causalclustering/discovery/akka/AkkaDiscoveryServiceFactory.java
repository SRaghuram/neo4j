/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.remote.artery.tcp.SSLEngineProvider;
import com.neo4j.causalclustering.discovery.AkkaDiscoverySSLEngineProvider;
import com.neo4j.causalclustering.discovery.DiscoveryFirstStartupDetector;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.discovery.member.CoreDiscoveryMemberFactory;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.CoreServerIdentity;

import java.time.Clock;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.internal.helpers.DefaultTimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;

public class AkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    private static final long RESTART_RETRY_DELAY_MS = 1000L;
    private static final long RESTART_RETRY_DELAY_MAX_MS = 60 * 1000L;
    private static final int RESTART_FAILURES_BEFORE_UNHEALTHY = 8;

    @Override
    public final AkkaCoreTopologyService coreTopologyService( Config config, CoreServerIdentity myIdentity, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
            RetryStrategy catchupAddressRetryStrategy,
            SslPolicyLoader sslPolicyLoader, CoreDiscoveryMemberFactory discoveryMemberFactory,
            DiscoveryFirstStartupDetector firstStartupDetector,
            Monitors monitors, Clock clock, DatabaseStateService databaseStateService )
    {
        TimeoutStrategy timeoutStrategy = exponential( RESTART_RETRY_DELAY_MS, RESTART_RETRY_DELAY_MAX_MS, MILLISECONDS );
        Restarter restarter = new Restarter( timeoutStrategy, RESTART_FAILURES_BEFORE_UNHEALTHY );

        return new AkkaCoreTopologyService(
                config,
                myIdentity,
                actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, firstStartupDetector ),
                logProvider,
                userLogProvider,
                catchupAddressRetryStrategy,
                restarter,
                discoveryMemberFactory,
                jobScheduler,
                clock,
                monitors,
                databaseStateService );
    }

    @Override
    public final AkkaTopologyClient readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
            ServerIdentity myIdentity, RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader,
            DiscoveryMemberFactory discoveryMemberFactory, Clock clock, DatabaseStateService databaseStateService )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                myIdentity,
                actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, new ReadReplicaDiscoveryFirstStartupDetector() ),
                discoveryMemberFactory,
                clock,
                jobScheduler,
                databaseStateService );
    }

    protected ActorSystemLifecycle actorSystemLifecycle( Config config, LogProvider logProvider, RemoteMembersResolver resolver,
                                                         SslPolicyLoader sslPolicyLoader,
                                                         DiscoveryFirstStartupDetector firstStartupDetector )
    {
        return new ActorSystemLifecycle(
                actorSystemFactory( sslPolicyLoader, firstStartupDetector, config, logProvider ),
                resolver,
                new JoinMessageFactory( resolver ),
                config,
                logProvider );
    }

    protected static ActorSystemFactory actorSystemFactory( SslPolicyLoader sslPolicyLoader, DiscoveryFirstStartupDetector firstStartupDetector,
                                                            Config config, LogProvider logProvider )
    {
        SslPolicy sslPolicy = sslPolicyLoader.hasPolicyForSource( CLUSTER ) ? sslPolicyLoader.getPolicy( CLUSTER ) : null;
        Optional<SSLEngineProvider> sslEngineProvider = Optional.ofNullable( sslPolicy ).map( AkkaDiscoverySSLEngineProvider::new );
        return new ActorSystemFactory( sslEngineProvider, firstStartupDetector, config, logProvider );
    }
}
