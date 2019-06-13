/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

class AkkaCoreTopologyServiceTest
{
    private Config config = Config.defaults();
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private LogProvider logProvider = NullLogProvider.getInstance();
    private LogProvider userLogProvider = NullLogProvider.getInstance();
    private RetryStrategy catchupAddressretryStrategy = new NoRetriesStrategy();
    private Clock clock = Clock.fixed( Instant.now(), ZoneId.of( "UTC" ) );
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    private ActorSystemLifecycle system = mock( ActorSystemLifecycle.class, Answers.RETURNS_DEEP_STUBS );

    private RetryStrategy restartRetryStrategy = new RetryStrategy( 0L, 10L );

    private AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            config,
            myself,
            system,
            logProvider,
            userLogProvider,
            catchupAddressretryStrategy,
            restartRetryStrategy,
            TestDiscoveryMember::new,
            executor,
            clock );

    @Test
    void shouldLifecycle() throws Throwable
    {
        service.init();
        service.start();

        verify( system ).createClusterActorSystem();
        verify( system, atLeastOnce() ).queueMostRecent( any() );
        verify( system, atLeastOnce() ).applicationActorOf( any(), any() );

        service.stop();
        service.shutdown();

        verify( system ).shutdown();
    }

    @Test
    void shouldNotRestartIfPre()
    {
        service.restart();

        verifyZeroInteractions( system );
    }

    @Test
    void shouldNotRestartIfIdle() throws Throwable
    {
        service.init();
        reset( system );

        service.restart();

        verifyZeroInteractions( system );
    }

    @Test
    void shouldNotRestartIfHalt() throws Throwable
    {
        service.init();
        service.start();
        service.stop();
        service.shutdown();
        reset( system );

        service.restart();

        verifyZeroInteractions( system );
    }

    @Test
    void shouldRestartIfRun() throws Throwable
    {
        service.init();
        service.start();
        reset( system );

        service.restart();

        InOrder inOrder = inOrder( system );
        inOrder.verify( system ).shutdown();
        inOrder.verify( system ).createClusterActorSystem();
    }

    @Test
    void shouldReturnRoleForLocalLeader()
    {
        var databaseId1 = databaseIdRepository.get( "customers" );
        var databaseId2 = databaseIdRepository.get( "orders" );

        var memberId1 = new MemberId( UUID.randomUUID() );
        var memberId2 = new MemberId( UUID.randomUUID() );

        var leaderInfo1 = new LeaderInfo( memberId1, 1 );
        var leaderInfo2 = new LeaderInfo( memberId2, 2 );

        service.setLeader( leaderInfo1, databaseId1 );
        service.setLeader( leaderInfo2, databaseId2 );

        assertEquals( RoleInfo.LEADER, service.coreRole( databaseId1, memberId1 ) );
        assertEquals( RoleInfo.LEADER, service.coreRole( databaseId2, memberId2 ) );

        assertEquals( RoleInfo.UNKNOWN, service.coreRole( databaseId1, memberId2 ) );
        assertEquals( RoleInfo.UNKNOWN, service.coreRole( databaseId2, memberId1 ) );
    }

    @Test
    void shouldReturnRoleForRemoteLeader()
    {
        var databaseId = databaseIdRepository.get( "customers" );
        var leaderId = new MemberId( UUID.randomUUID() );

        setupGlobalTopologyState( databaseId, leaderId );

        assertEquals( RoleInfo.LEADER, service.coreRole( databaseId, leaderId ) );
    }

    @Test
    void shouldReturnRoleForFollower()
    {
        var databaseId = databaseIdRepository.get( "customers" );
        var leaderId = new MemberId( UUID.randomUUID() );
        var followerId1 = new MemberId( UUID.randomUUID() );
        var followerId2 = new MemberId( UUID.randomUUID() );

        setupGlobalTopologyState( databaseId, leaderId, followerId1, followerId2 );

        assertEquals( RoleInfo.LEADER, service.coreRole( databaseId, leaderId ) );
        assertEquals( RoleInfo.FOLLOWER, service.coreRole( databaseId, followerId1 ) );
        assertEquals( RoleInfo.FOLLOWER, service.coreRole( databaseId, followerId2 ) );
    }

    @Test
    void shouldReturnRoleForUnknownDatabase()
    {
        var knownDatabaseId = databaseIdRepository.get( "customers" );
        var unknownDatabaseId = databaseIdRepository.get( "orders" );

        var leaderId = new MemberId( UUID.randomUUID() );
        var followerId = new MemberId( UUID.randomUUID() );

        setupGlobalTopologyState( knownDatabaseId, leaderId, followerId );

        assertEquals( RoleInfo.UNKNOWN, service.coreRole( unknownDatabaseId, leaderId ) );
        assertEquals( RoleInfo.UNKNOWN, service.coreRole( unknownDatabaseId, followerId ) );
    }

    @Test
    void shouldReturnRoleForUnknownMemberId()
    {
        var databaseId = databaseIdRepository.get( "customers" );
        var leaderId = new MemberId( UUID.randomUUID() );
        var followerId = new MemberId( UUID.randomUUID() );
        var unknownId = new MemberId( UUID.randomUUID() );

        setupGlobalTopologyState( databaseId, leaderId, followerId );

        assertEquals( RoleInfo.UNKNOWN, service.coreRole( databaseId, unknownId ) );
    }

    @Test
    void shouldNotBootstrapWhenEmpty()
    {
        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.defaultDatabase() ) );
        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.systemDatabase() ) );
        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.get( "customers" ) ) );
        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.get( "orders" ) ) );
    }

    @Test
    void shouldBootstrapKnownDatabase()
    {
        var databaseId = databaseIdRepository.get( "cars" );

        var bootstrapState = mock( BootstrapState.class );
        when( bootstrapState.canBootstrapRaft( databaseId ) ).thenReturn( true );
        service.topologyState().onBootstrapStateUpdate( bootstrapState );

        assertTrue( service.canBootstrapRaftGroup( databaseId ) );

        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.defaultDatabase() ) );
        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.systemDatabase() ) );
        assertFalse( service.canBootstrapRaftGroup( databaseIdRepository.get( "customers" ) ) );
    }

    private void setupGlobalTopologyState( DatabaseId databaseId, MemberId leaderId, MemberId... followerIds )
    {
        var topologyState = service.topologyState();

        var coreMembers = new HashMap<MemberId,CoreServerInfo>();

        if ( leaderId != null )
        {
            coreMembers.put( leaderId, addressesForCore( 0, false, Set.of( databaseId ) ) );
            topologyState.onDbLeaderUpdate( Map.of( databaseId, new LeaderInfo( leaderId, 42 ) ) );
        }

        if ( followerIds != null )
        {
            for ( int i = 0; i < followerIds.length; i++ )
            {
                coreMembers.put( followerIds[i], addressesForCore( i + 1, false, Set.of( databaseId ) ) );
            }
        }

        var coreTopology = new DatabaseCoreTopology( databaseId, new RaftId( UUID.randomUUID() ), coreMembers );
        topologyState.onTopologyUpdate( coreTopology );
    }

    @Test
    void shouldRetryRestartIfStopFails() throws Throwable
    {
        service.init();
        service.start();
        reset( system );

        Mockito.doThrow( new RuntimeException() ).when( system ).shutdown();

        service.restart();

        InOrder inOrder = inOrder( system );
        inOrder.verify( system ).shutdown();
        inOrder.verify( system ).createClusterActorSystem();
    }

    @Test
    void shouldRetryRestartIfStartFails() throws Throwable
    {
        service.init();
        service.start();
        reset( system );

        Mockito.doThrow( new RuntimeException() ).doNothing().when( system ).createClusterActorSystem();

        service.restart();

        InOrder inOrder = inOrder( system );
        inOrder.verify( system ).shutdown();
        inOrder.verify( system, times( 2 ) ).createClusterActorSystem();
    }
}
