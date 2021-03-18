/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;
import akka.remote.ThisActorSystemQuarantinedEvent;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryFirstStartupDetector;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.ActorSystemRestarter;
import com.neo4j.causalclustering.discovery.akka.AkkaActorSystemRestartStrategy;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.discovery.akka.AkkaTopologyClient;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.configuration.MinFormationMembers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.causalclustering.discovery.akka.system.AkkaDiscoverySystemHelper.NAMED_DATABASE_ID;
import static com.neo4j.causalclustering.discovery.akka.system.AkkaDiscoverySystemHelper.coreTopologyService;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
public class AkkaDiscoveryRestartIT
{
    private static final int CLUSTER_SIZE = 3;
    private static final long TIMEOUT = 60;

    private final List<TestAkkaCoreTopologyService> discoServices = new ArrayList<>();
    private final Random random = new Random();

    private TestAkkaCoreTopologyServiceFactory discoServiceFactory;

    @BeforeAll
    void setUp() throws Exception
    {
        int stablePort = PortAuthority.allocatePort();
        discoServiceFactory = new TestAkkaCoreTopologyServiceFactory();

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

    @Test
    void shouldNotTreatRestartAsFirstStartup()
    {
        // given
        discoServiceFactory.firstStartup = false;
        var restarter = discoServices.get( random.nextInt( CLUSTER_SIZE ) );
        Duration seedNodeTimeoutBefore =
                restarter.actorSystemLifecycle.actorSystemComponents.actorSystem().settings().config().getDuration( "akka.cluster.seed-node-timeout" );

        // when
        restarter.notifyRestart();

        // then
        assertEventually( restarter::isIdle, idleness -> idleness, TIMEOUT, SECONDS );
        assertEventually( restarter::isRunning, run -> run, TIMEOUT, SECONDS );

        Duration seedNodeTimeoutAfter =
                restarter.actorSystemLifecycle.actorSystemComponents.actorSystem().settings().config().getDuration( "akka.cluster.seed-node-timeout" );
        assertThat( seedNodeTimeoutAfter ).isGreaterThan( seedNodeTimeoutBefore );

        awaitClusterReformed();
    }

    private void awaitClusterReformed()
    {
        discoServices.forEach( this::awaitTopology );
    }

    private void awaitTopology( CoreTopologyService topologyService )
    {
        assertEventually( () -> topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ),
                topology -> topology.servers().size() == CLUSTER_SIZE,
                TIMEOUT, SECONDS );
    }

    private static class TestAkkaCoreTopologyServiceFactory implements DiscoveryServiceFactory
    {
        private Boolean firstStartup = true;

        @Override
        public TestAkkaCoreTopologyService coreTopologyService(
                Config config, CoreServerIdentity myIdentity, JobScheduler jobScheduler, LogProvider logProvider, LogProvider userLogProvider,
                RemoteMembersResolver remoteMembersResolver, RetryStrategy catchupAddressRetryStrategy, SslPolicyLoader sslPolicyLoader,
                ServerSnapshotFactory serverSnapshotFactory, DiscoveryFirstStartupDetector firstStartupDetector, Monitors monitors, Clock clock,
                DatabaseStateService databaseStateService, Panicker panicker )
        {
            ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forConfig( config);
            var minFormationMembers = MinFormationMembers.from( config );
            return new TestAkkaCoreTopologyService(
                    config,
                    myIdentity,
                    actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, minFormationMembers ),
                    logProvider,
                    userLogProvider,
                    catchupAddressRetryStrategy,
                    actorSystemRestarter,
                    serverSnapshotFactory,
                    jobScheduler,
                    clock,
                    monitors,
                    databaseStateService,
                    panicker
            );
        }

        @Override
        public AkkaTopologyClient readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
                ServerIdentity myIdentity, RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader,
                ServerSnapshotFactory serverSnapshotFactory, Clock clock, DatabaseStateService databaseStateService )
        {
            return null;
        }

        @Override
        public TopologyService standaloneTopologyService( Config config, ServerIdentity myIdentity, JobScheduler jobScheduler, LogProvider logProvider,
                                                          LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
                                                          RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader,
                                                          ServerSnapshotFactory serverSnapshotFactory, Monitors monitors, Clock clock,
                                                          DatabaseStateService databaseStateService, Panicker panicker )
        {
            throw new UnsupportedOperationException();
        }

        protected ActorSystemLifecycle actorSystemLifecycle( Config config, LogProvider logProvider, RemoteMembersResolver resolver,
                SslPolicyLoader sslPolicyLoader, MinFormationMembers minFormationMembers )
        {
            return new ActorSystemLifecycle(
                    actorSystemFactory( sslPolicyLoader, config, logProvider, minFormationMembers ),
                    resolver,
                    new JoinMessageFactory( resolver ),
                    config,
                    logProvider, minFormationMembers,
                    new AkkaActorSystemRestartStrategy.RestartWhenMajorityUnreachableOrSingletonFirstSeed( resolver ) );
        }

        protected ActorSystemFactory actorSystemFactory( SslPolicyLoader ignored, Config config, LogProvider logProvider ,
                                                         MinFormationMembers minFormationMembers )
        {
            return new ActorSystemFactory( Optional.empty(), new TestFirstStartupDetector( () -> this.firstStartup ), config, logProvider,
                                           minFormationMembers );
        }
    }

    private static class TestAkkaCoreTopologyService extends AkkaCoreTopologyService
    {
        private final ActorSystemLifecycle actorSystemLifecycle;

        TestAkkaCoreTopologyService(
                Config config, CoreServerIdentity identityModule, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
                LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, ActorSystemRestarter actorSystemRestarter,
                ServerSnapshotFactory serverSnapshotFactory, JobScheduler jobScheduler, Clock clock, Monitors monitors,
                DatabaseStateService databaseStateService, Panicker panicker )
        {
            super( config, identityModule, actorSystemLifecycle, logProvider, userLogProvider, catchupAddressRetryStrategy, actorSystemRestarter,
                   serverSnapshotFactory, jobScheduler, clock, monitors, databaseStateService, panicker );
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
