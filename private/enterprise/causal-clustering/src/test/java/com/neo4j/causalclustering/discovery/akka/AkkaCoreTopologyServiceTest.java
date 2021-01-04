/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.TestCoreServerSnapshot;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

import static com.neo4j.causalclustering.discovery.akka.GlobalTopologyStateTestUtil.setupCoreTopologyState;
import static com.neo4j.causalclustering.discovery.akka.GlobalTopologyStateTestUtil.setupLeader;
import static com.neo4j.causalclustering.discovery.akka.GlobalTopologyStateTestUtil.setupRaftMapping;
import static com.neo4j.causalclustering.discovery.akka.GlobalTopologyStateTestUtil.setupReadReplicaTopologyState;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class AkkaCoreTopologyServiceTest
{
    private Config config = Config.defaults();
    private CoreServerIdentity myIdentity = new InMemoryCoreServerIdentity();
    private LogProvider logProvider = NullLogProvider.getInstance();
    private LogProvider userLogProvider = NullLogProvider.getInstance();
    private RetryStrategy catchupAddressretryStrategy = new NoRetriesStrategy();
    private Clock clock = Clock.fixed( Instant.now(), ZoneId.of( "UTC" ) );
    private final int maxAkkaSystemRestartAttempts = 3;
    private ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forTest( maxAkkaSystemRestartAttempts );

    private Map<NamedDatabaseId,DatabaseState> databaseStates;
    private DatabaseStateService databaseStateService;
    private ActorSystem system;
    private ActorSystemLifecycle systemLifecycle;
    private TestKit testKit;
    private AkkaCoreTopologyService service;

    @BeforeEach
    void setup()
    {
        var databaseId1 = randomNamedDatabaseId();
        var databaseId2 = randomNamedDatabaseId();
        databaseStates = Map.of(
                databaseId1, new EnterpriseDatabaseState( databaseId1, STARTED ),
                databaseId2, new EnterpriseDatabaseState( databaseId2, STOPPED ) );
        databaseStateService = new StubDatabaseStateService( databaseStates, EnterpriseDatabaseState::unknown );

        system = ActorSystem.create();
        systemLifecycle = mock( ActorSystemLifecycle.class );
        testKit = new TestKit( system );

        when( systemLifecycle.applicationActorOf( any( Props.class ), anyString() ) )
                .then( i ->
                {
                    var name = i.getArgument( 1, String.class );
                    return testKit.getRef();
                } );

        service = new AkkaCoreTopologyService(
                config,
                myIdentity,
                systemLifecycle,
                logProvider,
                userLogProvider,
                catchupAddressretryStrategy,
                actorSystemRestarter,
                TestCoreServerSnapshot::factory,
                new JobSchedulerAdapter(),
                clock,
                new Monitors(),
                databaseStateService,
                DummyPanicService.PANICKER
        );
    }

    @AfterEach
    void tearDown()
    {
        TestKit.shutdownActorSystem( system );
        system = null;
    }

    @Test
    void shouldPublishInitialDataOnRestart() throws Throwable
    {
        service.init();
        service.start();

        var expectedSnapshot = TestCoreServerSnapshot.factory( myIdentity, databaseStateService, Map.of() );
        var expectedMessage = new PublishInitialData( expectedSnapshot );
        testKit.expectMsgEquals( expectedMessage );

        assertTrue( service.restartSameThread() );

        testKit.expectMsgEquals( expectedMessage );
    }

    @Test
    void shouldLifecycle() throws Throwable
    {
        service.init();
        service.start();

        verify( systemLifecycle ).createClusterActorSystem( any() );
        verify( systemLifecycle, atLeastOnce() ).queueMostRecent( any() );
        verify( systemLifecycle, atLeastOnce() ).applicationActorOf( any(), any() );

        service.stop();
        service.shutdown();

        verify( systemLifecycle ).shutdown();
    }

    @Test
    void shouldNotRestartIfPre()
    {
        assertFalse( service.restartSameThread() );

        verifyNoInteractions( systemLifecycle );
    }

    @Test
    void shouldNotRestartIfIdle() throws Throwable
    {
        service.init();
        clearInvocations( systemLifecycle );

        assertFalse( service.restartSameThread() );

        verifyNoInteractions( systemLifecycle );
    }

    @Test
    void shouldNotRestartIfHalt() throws Throwable
    {
        service.init();
        service.start();
        service.stop();
        service.shutdown();
        clearInvocations( systemLifecycle );

        assertFalse( service.restartSameThread() );

        verifyNoInteractions( systemLifecycle );
    }

    @Test
    void shouldRestartIfRun() throws Throwable
    {
        service.init();
        service.start();
        clearInvocations( systemLifecycle );

        assertTrue( service.restartSameThread() );

        InOrder inOrder = inOrder( systemLifecycle );
        inOrder.verify( systemLifecycle ).shutdown();
        inOrder.verify( systemLifecycle ).createClusterActorSystem( any() );
    }

    @Test
    void shouldReturnRoleForLocalLeader()
    {
        var databaseId1 = randomNamedDatabaseId();
        var databaseId2 = randomNamedDatabaseId();

        var coreA = new InMemoryCoreServerIdentity();
        var coreB = new InMemoryCoreServerIdentity();

        setupRaftMapping( service.topologyState(), databaseId1, Map.of( coreA.serverId(), coreA.raftMemberId( databaseId1 ) ) );
        setupRaftMapping( service.topologyState(), databaseId2, Map.of( coreB.serverId(), coreB.raftMemberId( databaseId2 ) ) );

        var leaderInfo1 = new LeaderInfo( coreA.raftMemberId( databaseId1 ), 1 );
        var leaderInfo2 = new LeaderInfo( coreB.raftMemberId( databaseId2 ), 2 );

        service.setLeader( leaderInfo1, databaseId1 );
        service.setLeader( leaderInfo2, databaseId2 );

        assertEquals( RoleInfo.LEADER, service.lookupRole( databaseId1, coreA.serverId() ) );
        assertEquals( RoleInfo.LEADER, service.lookupRole( databaseId2, coreB.serverId() ) );

        assertEquals( RoleInfo.UNKNOWN, service.lookupRole( databaseId1, coreB.serverId() ) );
        assertEquals( RoleInfo.UNKNOWN, service.lookupRole( databaseId2, coreA.serverId() ) );
    }

    @Test
    void shouldReturnRoleForRemoteLeader()
    {
        var databaseId = randomNamedDatabaseId();
        var leaderId = IdFactory.randomServerId();
        var leaderRaftId = IdFactory.randomRaftMemberId();

        setupCoreTopologyState( service.topologyState(), databaseId, leaderId );
        setupLeader( service.topologyState(), databaseId, leaderRaftId );
        setupRaftMapping( service.topologyState(), databaseId, Map.of( leaderId, leaderRaftId ) );

        assertEquals( RoleInfo.LEADER, service.lookupRole( databaseId, leaderId ) );
    }

    @Test
    void shouldReturnRoleForFollower()
    {
        var databaseId = randomNamedDatabaseId();
        var leaderId = IdFactory.randomServerId();
        var leaderRaftMemberId = IdFactory.randomRaftMemberId();
        var followerId1 = IdFactory.randomServerId();
        var followerId2 = IdFactory.randomServerId();

        setupCoreTopologyState( service.topologyState(), databaseId, leaderId, followerId1, followerId2 );
        setupLeader( service.topologyState(), databaseId, leaderRaftMemberId );
        setupRaftMapping( service.topologyState(), databaseId,
                Map.of( leaderId, leaderRaftMemberId, followerId1, IdFactory.randomRaftMemberId(), followerId2, IdFactory.randomRaftMemberId() ) );

        assertEquals( RoleInfo.LEADER, service.lookupRole( databaseId, leaderId ) );
        assertEquals( RoleInfo.FOLLOWER, service.lookupRole( databaseId, followerId1 ) );
        assertEquals( RoleInfo.FOLLOWER, service.lookupRole( databaseId, followerId2 ) );
    }

    @Test
    void shouldReturnRoleForUnknownDatabase()
    {
        var knownDatabaseId = randomNamedDatabaseId();
        var unknownDatabaseId = randomNamedDatabaseId();

        var leaderId = IdFactory.randomServerId();
        var followerId = IdFactory.randomServerId();

        setupCoreTopologyState( service.topologyState(), knownDatabaseId, leaderId, followerId );

        assertEquals( RoleInfo.UNKNOWN, service.lookupRole( unknownDatabaseId, leaderId ) );
        assertEquals( RoleInfo.UNKNOWN, service.lookupRole( unknownDatabaseId, followerId ) );
    }

    @Test
    void shouldReturnRoleForUnknownMemberId()
    {
        var databaseId = randomNamedDatabaseId();
        var leaderId = IdFactory.randomServerId();
        var followerId = IdFactory.randomServerId();
        var unknownId = IdFactory.randomServerId();

        setupCoreTopologyState( service.topologyState(), databaseId, leaderId, followerId );

        assertEquals( RoleInfo.UNKNOWN, service.lookupRole( databaseId, unknownId ) );
    }

    @Test
    void shouldReturnRoleWhenDatabaseStopped()
    {
        var databaseId = randomNamedDatabaseId();
        var leaderId = IdFactory.randomServerId();
        var leaderInfo = new LeaderInfo( service.topologyState().resolveRaftMemberForServer( databaseId.databaseId(), leaderId ), 1 );

        service.setLeader( leaderInfo, databaseId );
        assertEquals( RoleInfo.LEADER, service.lookupRole( databaseId, leaderId ) );

        service.onDatabaseStop( databaseId );
        assertEquals( RoleInfo.UNKNOWN, service.lookupRole( databaseId, leaderId ) );
    }

    @Test
    void shouldNotBootstrapWhenEmpty()
    {
        assertFalse( service.canBootstrapDatabase( randomNamedDatabaseId() ) );
        assertFalse( service.canBootstrapDatabase( NAMED_SYSTEM_DATABASE_ID ) );
    }

    @Test
    void shouldBootstrapKnownDatabase()
    {
        var databaseId = randomNamedDatabaseId();

        var bootstrapState = mock( BootstrapState.class );
        when( bootstrapState.canBootstrapRaft( databaseId ) ).thenReturn( true );
        service.topologyState().onBootstrapStateUpdate( bootstrapState );

        assertTrue( service.canBootstrapDatabase( databaseId ) );

        assertFalse( service.canBootstrapDatabase( randomNamedDatabaseId() ) );
        assertFalse( service.canBootstrapDatabase( NAMED_SYSTEM_DATABASE_ID ) );
    }

    @Test
    void shouldCorrectlyReportIfBootstrappedDatabase()
    {
        var databaseId = randomNamedDatabaseId();

        var bootstrapState = mock( BootstrapState.class );
        var raftMemberId = service.resolveRaftMemberForServer( databaseId, service.serverId() );
        when( bootstrapState.memberBootstrappedRaft( databaseId, raftMemberId ) ).thenReturn( true );
        service.topologyState().onBootstrapStateUpdate( bootstrapState );

        assertTrue( service.didBootstrapDatabase( databaseId ) );

        assertFalse( service.canBootstrapDatabase( randomNamedDatabaseId() ) );
        assertFalse( service.canBootstrapDatabase( NAMED_SYSTEM_DATABASE_ID ) );
    }

    @Test
    void shouldReportEmptyTopologiesWhenShutdown() throws Exception
    {
        testEmptyTopologiesAreReportedAfter( topologyService ->
        {
            topologyService.stop();
            topologyService.shutdown();
        } );
    }

    @Test
    void shouldReportEmptyTopologiesAfterRestart() throws Exception
    {
        testEmptyTopologiesAreReportedAfter( AkkaCoreTopologyService::restartSameThread );
    }

    @Test
    void shouldRetryRestartIfStopFails() throws Throwable
    {
        service.init();
        service.start();
        clearInvocations( systemLifecycle );

        Mockito.doThrow( new RuntimeException() ).when( systemLifecycle ).shutdown();

        assertTrue( service.restartSameThread() );

        InOrder inOrder = inOrder( systemLifecycle );
        inOrder.verify( systemLifecycle ).shutdown();
        inOrder.verify( systemLifecycle ).createClusterActorSystem( any() );
    }

    @Test
    void shouldRetryRestartIfStartFails() throws Throwable
    {
        service.init();
        service.start();
        clearInvocations( systemLifecycle );

        Mockito.doThrow( new RuntimeException() ).doNothing().when( systemLifecycle ).createClusterActorSystem( any() );

        assertTrue( service.restartSameThread() );

        InOrder inOrder = inOrder( systemLifecycle );
        inOrder.verify( systemLifecycle ).shutdown();
        inOrder.verify( systemLifecycle, times( 2 ) ).createClusterActorSystem( any() );
    }

    @Test
    void shouldRetryUntilSucceeded() throws Throwable
    {
        service.init();
        service.start();
        clearInvocations( systemLifecycle );

        int numFailures = maxAkkaSystemRestartAttempts - 1;
        Exception exception = new RuntimeException();
        final Exception[] exceptions = Stream.generate( () -> exception ).limit( numFailures ).toArray( Exception[]::new );
        Mockito.doThrow( exceptions ).doNothing().when( systemLifecycle ).createClusterActorSystem( any() );

        assertTrue( service.restartSameThread() );

        InOrder inOrder = inOrder( systemLifecycle );
        inOrder.verify( systemLifecycle ).shutdown();
        inOrder.verify( systemLifecycle, times( maxAkkaSystemRestartAttempts ) ).createClusterActorSystem( any() );
    }

    @Test
    void shouldRetryUntilFailuresExceeded() throws Throwable
    {
        service.init();
        service.start();
        clearInvocations( systemLifecycle );

        int numFailures = maxAkkaSystemRestartAttempts;
        Exception exception = new RuntimeException();
        final Exception[] exceptions = Stream.generate( () -> exception ).limit( numFailures ).toArray( Exception[]::new );
        Mockito.doThrow( exceptions ).doNothing().when( systemLifecycle ).createClusterActorSystem( any() );

        assertThatThrownBy( service::restartSameThread ).hasCauseInstanceOf( ActorSystemRestarter.RestartFailedException.class );

        InOrder inOrder = inOrder( systemLifecycle );
        inOrder.verify( systemLifecycle ).shutdown();
        inOrder.verify( systemLifecycle, times( maxAkkaSystemRestartAttempts ) ).createClusterActorSystem( any() );
    }

    private void testEmptyTopologiesAreReportedAfter( ThrowingConsumer<AkkaCoreTopologyService,Exception> testAction ) throws Exception
    {
        var databaseId = randomNamedDatabaseId();
        var memberId1 = IdFactory.randomServerId();
        var memberId2 = IdFactory.randomServerId();
        var memberId3 = IdFactory.randomServerId();

        service.init();
        service.start();

        // setup fake topology for cores
        var bootstrapState = mock( BootstrapState.class );
        when( bootstrapState.canBootstrapRaft( databaseId ) ).thenReturn( true );
        service.topologyState().onBootstrapStateUpdate( bootstrapState );
        setupCoreTopologyState( service.topologyState(), databaseId, memberId1, memberId2, memberId3 );

        // setup fake topology for read replicas
        setupReadReplicaTopologyState( service.topologyState(), databaseId, memberId1, memberId2 );

        // verify core topology is not empty
        assertEquals( Set.of( memberId1, memberId2, memberId3 ), service.coreTopologyForDatabase( databaseId ).servers().keySet() );
        assertEquals( Set.of( memberId1, memberId2, memberId3 ), service.allCoreServers().keySet() );
        assertTrue( service.canBootstrapDatabase( databaseId ) );

        // verify read replica topology is not empty
        assertEquals( Set.of( memberId1, memberId2 ), service.readReplicaTopologyForDatabase( databaseId ).servers().keySet() );
        assertEquals( Set.of( memberId1, memberId2 ), service.allReadReplicas().keySet() );

        testAction.accept( service );

        // verify core topology is empty
        assertThat( service.coreTopologyForDatabase( databaseId ).servers().keySet(), is( empty() ) );
        assertThat( service.allCoreServers().keySet(), is( empty() ) );
        assertFalse( service.canBootstrapDatabase( databaseId ) );

        // verify read replica topology is empty
        assertThat( service.readReplicaTopologyForDatabase( databaseId ).servers().keySet(), is( empty() ) );
        assertThat( service.allCoreServers().keySet(), is( empty() ) );
    }
}
