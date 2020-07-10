/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;
import akka.remote.ThisActorSystemQuarantinedEvent;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.AkkaTopologyClient;
import com.neo4j.causalclustering.discovery.akka.Restarter;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.causalclustering.discovery.akka.system.AkkaDiscoverySystemHelper.NAMED_DATABASE_ID;
import static com.neo4j.causalclustering.discovery.akka.system.AkkaDiscoverySystemHelper.coreTopologyService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
public class AkkaDiscoveryRestartIT
{
    private static final int CLUSTER_SIZE = 3;
    private static final long TIMEOUT = 60;

    private final List<TestAkkaCoreTopologyService> discoServices = new ArrayList<>();
    private final Random random = new Random();

    @BeforeAll
    void setUp() throws Exception
    {
        int stablePort = PortAuthority.allocatePort();
        var discoServiceFactory = new TestAkkaCoreTopologyServiceFactory();

        discoServices.add( (TestAkkaCoreTopologyService) coreTopologyService( stablePort, discoServiceFactory, stablePort ) );

        for ( int i = 0; i < CLUSTER_SIZE - 1; i++ )
        {
            discoServices.add( (TestAkkaCoreTopologyService) coreTopologyService( stablePort, discoServiceFactory ) );
        }

        for ( var topologyService : discoServices )
        {
            topologyService.init();
            topologyService.start();
        }

        awaitClusterReformed();
    }

    @Test
    void shouldSuccessfullyRestart()
    {
        var restarter = discoServices.get( random.nextInt( CLUSTER_SIZE ) );

        restarter.notifyRestart();

        assertEventually( restarter::isIdle, idleness -> idleness, TIMEOUT, SECONDS );
        assertEventually( restarter::isRunning, run -> run, TIMEOUT, SECONDS );

        awaitClusterReformed();
    }

    private void awaitClusterReformed()
    {
        discoServices.forEach( this::awaitTopology );
    }

    private void awaitTopology( CoreTopologyService topologyService )
    {
        assertEventually( () -> topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ),
                topology -> topology.members().size() == CLUSTER_SIZE,
                TIMEOUT, SECONDS );
    }

    private static class TestAkkaCoreTopologyServiceFactory implements DiscoveryServiceFactory
    {
        private static final long RESTART_RETRY_DELAY_MS = 1000L;
        private static final long RESTART_RETRY_DELAY_MAX_MS = 60 * 1000L;
        private static final int RESTART_FAILURES_BEFORE_UNHEALTHY = 8;

        @Override
        public TestAkkaCoreTopologyService coreTopologyService( Config config, ClusteringIdentityModule identityModule, JobScheduler jobScheduler,
                LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver, RetryStrategy catchupAddressRetryStrategy,
                SslPolicyLoader sslPolicyLoader, DiscoveryMemberFactory discoveryMemberFactory, Monitors monitors, Clock clock )
        {
            TimeoutStrategy timeoutStrategy = new ExponentialBackoffStrategy( RESTART_RETRY_DELAY_MS, RESTART_RETRY_DELAY_MAX_MS, MILLISECONDS );
            Restarter restarter = new Restarter( timeoutStrategy, RESTART_FAILURES_BEFORE_UNHEALTHY );

            return new TestAkkaCoreTopologyService(
                    config,
                    identityModule,
                    actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader ),
                    logProvider,
                    userLogProvider, catchupAddressRetryStrategy,
                    restarter,
                    discoveryMemberFactory,
                    Executors.newCachedThreadPool(),
                    clock,
                    monitors );
        }

        @Override
        public AkkaTopologyClient readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
                ClusteringIdentityModule identityModule, RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader,
                DiscoveryMemberFactory discoveryMemberFactory, Clock clock )
        {
            return null;
        }

        protected ActorSystemLifecycle actorSystemLifecycle( Config config, LogProvider logProvider, RemoteMembersResolver resolver,
                SslPolicyLoader sslPolicyLoader )
        {
            return new ActorSystemLifecycle(
                    actorSystemFactory( sslPolicyLoader, config, logProvider ),
                    resolver,
                    new JoinMessageFactory( resolver ),
                    config,
                    logProvider );
        }

        protected static ActorSystemFactory actorSystemFactory( SslPolicyLoader ignored, Config config, LogProvider logProvider )
        {
            return new ActorSystemFactory( Optional.empty(), config, logProvider );
        }
    }

    private static class TestAkkaCoreTopologyService extends AkkaCoreTopologyService
    {
        private final ActorSystemLifecycle actorSystemLifecycle;

        TestAkkaCoreTopologyService( Config config, ClusteringIdentityModule identityModule, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
                LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, Restarter restarter, DiscoveryMemberFactory discoveryMemberFactory,
                Executor executor, Clock clock, Monitors monitors )
        {
            super( config, identityModule, actorSystemLifecycle, logProvider, userLogProvider, catchupAddressRetryStrategy, restarter, discoveryMemberFactory,
                    executor, clock, monitors );
            this.actorSystemLifecycle = actorSystemLifecycle;
        }

        public void notifyRestart()
        {
            Address address = new Address( "akka", "system" );
            actorSystemLifecycle.eventStream().publish( new ThisActorSystemQuarantinedEvent( address, address ) );
        }

        public boolean isIdle()
        {
            return state() == State.IDLE;
        }

        public boolean isRunning()
        {
            return state() == State.RUN;
        }
    }
}
