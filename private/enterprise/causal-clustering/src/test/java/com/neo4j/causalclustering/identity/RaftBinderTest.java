/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.RaftBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.storage.InMemorySimpleStorage;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.PublishRaftIdOutcome;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class RaftBinderTest
{
    private static final NamedDatabaseId SOME_NAMED_DATABASE_ID = randomNamedDatabaseId();
    private static final NamedDatabaseId DEFAULT_DATABASE_ID = DatabaseIdFactory.from( DEFAULT_DATABASE_NAME, UUID.randomUUID() );

    private static final StoreId SOME_STORE_ID = new StoreId( 0, 0, 0 );

    private final RaftBootstrapper raftBootstrapper = mock( RaftBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    private final Config config = Config.defaults();
    private final int minCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
    private final RaftMemberId meAsRaftMember = IdFactory.randomRaftMemberId();
    private final MemberId meAsServer = MemberId.of( meAsRaftMember );

    private Set<UUID> extractMemberUUIDs( Map<MemberId,CoreServerInfo> members )
    {
        return members.keySet().stream().map( MemberId::getUuid ).collect( toSet() );
    }

    private ClusterSystemGraphDbmsModel systemGraphFor( NamedDatabaseId namedDatabaseId, Set<UUID> initialMembers )
    {
        ClusterSystemGraphDbmsModel systemGraph = mock( ClusterSystemGraphDbmsModel.class );
        when( systemGraph.getStoreId( namedDatabaseId ) ).thenReturn( SOME_STORE_ID );
        when( systemGraph.getInitialMembers( namedDatabaseId ) ).thenReturn( initialMembers );
        return systemGraph;
    }

    private DatabaseStartAborter neverAbort()
    {
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false );
        return aborter;
    }

    private RaftBinder raftBinder( SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService )
    {
        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, emptySet() );
        return new RaftBinder( SOME_NAMED_DATABASE_ID, meAsRaftMember, meAsServer,raftIdStorage, topologyService, systemGraph, clock,
                () -> clock.forward( 1, TimeUnit.SECONDS ), Duration.ofSeconds( 10 ), raftBootstrapper, minCoreHosts, false, new Monitors() );
    }

    private RaftBinder raftBinder( SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService, NamedDatabaseId namedDatabaseId,
            ClusterSystemGraphDbmsModel systemGraph )
    {
        return new RaftBinder( namedDatabaseId, meAsRaftMember, meAsServer, raftIdStorage, topologyService, systemGraph, clock,
                () -> clock.forward( 1, TimeUnit.SECONDS ), Duration.ofSeconds( 10 ), raftBootstrapper, minCoreHosts, false, new Monitors() );
    }

    @Test
    void shouldThrowOnAbort()
    {
        // given
        var unboundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, emptyMap() );
        var topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( unboundTopology );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( true );

        // when / then
        assertThrows( DatabaseStartAbortedException.class, () -> binder.bindToRaft( aborter ) );
    }

    @Test
    void shouldThrowOnRaftIdDatabaseIdMismatch()
    {
        // given
        var previouslyBoundRaftId = IdFactory.randomRaftId();

        var raftIdStorage = new InMemorySimpleStorage<RaftId>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        var binder = raftBinder( raftIdStorage, mock( CoreTopologyService.class ) );
        var exception = IllegalStateException.class;

        // when / then
        assertThrows( exception, () -> binder.bindToRaft( neverAbort() ) );
    }

    @Test
    void shouldTimeoutWhenNotBootstrappableAndNobodyElsePublishesRaftId()
    {
        // given
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, emptyMap() );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( unboundTopology );

        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );

        // when / then
        assertThrows( TimeoutException.class, () -> binder.bindToRaft( neverAbort() ) );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( SOME_NAMED_DATABASE_ID );
    }

    @Test
    void shouldBindToRaftIdPublishedByAnotherMember() throws Throwable
    {
        // given
        RaftId publishedRaftId = RaftId.from( SOME_NAMED_DATABASE_ID.databaseId() );
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, emptyMap() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_NAMED_DATABASE_ID, systemGraph );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( publishedRaftId, raftId.get() );
        verify( topologyService, never() ).publishRaftId( publishedRaftId, meAsRaftMember );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( SOME_NAMED_DATABASE_ID );
    }

    @Test
    void shouldPublishStoredRaftIdIfPreviouslyBound() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( SOME_NAMED_DATABASE_ID.databaseId() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.publishRaftId( previouslyBoundRaftId, meAsRaftMember ) ).thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        verify( topologyService ).publishRaftId( previouslyBoundRaftId, meAsRaftMember );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( previouslyBoundRaftId, raftId.get() );
    }

    @Test
    void shouldRetryWhenPublishOfStoredRaftIdFailsWithTransientErrors() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( SOME_NAMED_DATABASE_ID.databaseId() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.publishRaftId( previouslyBoundRaftId, meAsRaftMember ) )
                .thenReturn( PublishRaftIdOutcome.FAILED_PUBLISH )
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        verify( topologyService, atLeast( 2 ) ).publishRaftId( previouslyBoundRaftId, meAsRaftMember );
    }

    @Test
    void shouldNotRetryWhenOtherMemberPublishesStoredRaftId() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( SOME_NAMED_DATABASE_ID.databaseId() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.publishRaftId( previouslyBoundRaftId, meAsRaftMember ) )
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_OTHER );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        verify( topologyService, times( 1 ) ).publishRaftId( previouslyBoundRaftId, meAsRaftMember );
    }

    @Test
    void shouldRetryWhenPublishFailsWithTransientErrors() throws Throwable
    {
        // given
        var namedDatabaseId = randomNamedDatabaseId();
        var members = IntStream.range( 0, minCoreHosts ).boxed().collect( Collectors.toMap(
                        i -> IdFactory.randomMemberId(),
                        i -> addressesForCore( i, false, singleton( namedDatabaseId.databaseId() ) ) ) );
        var raftMembers = members.keySet().stream().map( RaftMemberId::from ).collect( Collectors.toSet() );

        var bootstrappableTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), null, members );
        var topologyService = mock( CoreTopologyService.class );

        when( topologyService.publishRaftId( any( RaftId.class ), any( RaftMemberId.class ) ) )
                .thenReturn( PublishRaftIdOutcome.FAILED_PUBLISH ) // Cause first retry
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_OTHER ) // Cause second retry
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME ); // Finally succeed
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( bootstrappableTopology );
        when( topologyService.canBootstrapRaftGroup( namedDatabaseId ) ).thenReturn( true );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers ) ).thenReturn( snapshot );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, namedDatabaseId, systemGraphFor( namedDatabaseId, emptySet() ) );

        // when
        binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService, atLeast( 3 ) ).publishRaftId( RaftId.from( namedDatabaseId.databaseId() ), meAsRaftMember );
    }

    @Test
    void shouldTimeoutIfPublishRaftIdContinuallyFailsWithTransientErrors() throws Throwable
    {
        // given
        var namedDatabaseId = randomNamedDatabaseId();
        var members = IntStream.range( 0, minCoreHosts ).boxed().collect( Collectors.toMap(
                i -> IdFactory.randomMemberId(),
                i -> addressesForCore( i, false, singleton( namedDatabaseId.databaseId() ) ) ) );

        var bootstrappableTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), null, members );
        var topologyService = mock( CoreTopologyService.class );
        var raftMembers = members.keySet().stream().map( RaftMemberId::from ).collect( Collectors.toSet() );

        when( topologyService.publishRaftId( any( RaftId.class ), any( RaftMemberId.class ) ) ).thenReturn( PublishRaftIdOutcome.FAILED_PUBLISH );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( bootstrappableTopology );
        when( topologyService.canBootstrapRaftGroup( namedDatabaseId ) ).thenReturn( true );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers ) ).thenReturn( snapshot );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, namedDatabaseId, systemGraphFor( namedDatabaseId, emptySet() ) );

        // when / then
        assertThrows( TimeoutException.class, () -> binder.bindToRaft( neverAbort() ) );
        verify( topologyService, atLeast( 1 ) ).publishRaftId( RaftId.from( namedDatabaseId.databaseId() ), meAsRaftMember );
    }

    static Stream<Arguments> initialDatabases()
    {
        return Stream.of(
                Arguments.of( NAMED_SYSTEM_DATABASE_ID, "system" ),
                Arguments.of( DEFAULT_DATABASE_ID, "neo4j" ),
                Arguments.of( DatabaseIdFactory.from( "other_default", UUID.randomUUID() ), "other_default" )
        );
    }

    @ParameterizedTest( name = "{1}" )
    @MethodSource( "initialDatabases" )
    void shouldBootstrapInitialDatabasesUsingDiscoveryMethod( NamedDatabaseId namedDatabaseId, @SuppressWarnings( "unused" ) String databaseName )
            throws Throwable
    {
        // given
        var members = IntStream.range( 0, minCoreHosts ).boxed()
                .collect( Collectors.toMap( i -> IdFactory.randomMemberId(), i -> addressesForCore( i, false ) ) );
        var raftMembers = members.keySet().stream().map( RaftMemberId::from ).collect( Collectors.toSet() );

        DatabaseCoreTopology topology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), null, members );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( topology );
        when( topologyService.publishRaftId( any(), any() ) ).thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );
        when( topologyService.canBootstrapRaftGroup( namedDatabaseId ) ).thenReturn( true );
        members.keySet().forEach(
                serverId -> when( topologyService.resolveRaftMemberForServer( namedDatabaseId, serverId ) ).thenReturn( RaftMemberId.from( serverId ) ) );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers ) ).thenReturn( snapshot );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( namedDatabaseId, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, namedDatabaseId, systemGraph );

        // when
        BoundState boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( raftBootstrapper ).bootstrap( raftMembers );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), namedDatabaseId.databaseId().uuid() );
        verify( topologyService ).publishRaftId( raftId.get(), meAsRaftMember );
        assertHasSnapshot( boundState );
        assertEquals( snapshot, boundState.snapshot().get() );
    }

    private void assertHasSnapshot( BoundState boundState )
    {
        assertTrue( boundState.snapshot().isPresent() );
    }

    @Test
    void shouldBootstrapNonInitialDatabaseUsingSystemDatabaseMethod() throws Throwable
    {
        // given
        var members = IntStream.range( 0, minCoreHosts - 1 )
                .mapToObj( i -> Pair.of( IdFactory.randomMemberId(), addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
        members.put( meAsServer, addressesForCore( minCoreHosts, false ) );
        var raftMembers = members.keySet().stream().map( RaftMemberId::from ).collect( Collectors.toSet() );

        DatabaseCoreTopology topology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, members );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( topology );
        when( topologyService.publishRaftId( any(), any() ) ).thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );
        when( topologyService.canBootstrapRaftGroup( SOME_NAMED_DATABASE_ID ) ).thenReturn( true );
        members.keySet().forEach( serverId -> when( topologyService.resolveRaftMemberForServer( SOME_NAMED_DATABASE_ID, serverId ) )
                .thenReturn( RaftMemberId.from( serverId ) ) );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers, SOME_STORE_ID ) ).thenReturn( snapshot );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, extractMemberUUIDs( members ) );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_NAMED_DATABASE_ID, systemGraph );

        // when
        BoundState boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( raftBootstrapper ).bootstrap( raftMembers, SOME_STORE_ID );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), SOME_NAMED_DATABASE_ID.databaseId().uuid() );
        verify( topologyService ).publishRaftId( raftId.get(), meAsRaftMember );
        assertHasSnapshot( boundState );
        assertEquals( snapshot, boundState.snapshot().get() );
    }

    @Test
    void shouldNotBootstrapDatabaseWhenRaftIdAlreadyPublished() throws Exception
    {
        // given
        RaftId publishedRaftId = RaftId.from( SOME_NAMED_DATABASE_ID.databaseId() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_NAMED_DATABASE_ID, systemGraph );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService ).coreTopologyForDatabase( SOME_NAMED_DATABASE_ID );
        verifyNoInteractions( raftBootstrapper );

        Optional<RaftId> raftId = binder.get();
        assertEquals( Optional.of( publishedRaftId ), raftId );
        assertNoSnapshot( boundState );
    }

    @Test
    void shouldNotBootstrapSystemDatabaseWhenRaftIdAlreadyPublished() throws Exception
    {
        // given
        RaftId publishedRaftId = RaftId.from( NAMED_SYSTEM_DATABASE_ID.databaseId() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( NAMED_SYSTEM_DATABASE_ID.databaseId(), publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( NAMED_SYSTEM_DATABASE_ID ) ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( NAMED_SYSTEM_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, NAMED_SYSTEM_DATABASE_ID, systemGraph );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService ).coreTopologyForDatabase( NAMED_SYSTEM_DATABASE_ID );
        verify( raftBootstrapper ).saveStore();
        verifyNoMoreInteractions( raftBootstrapper );

        Optional<RaftId> raftId = binder.get();
        assertNoSnapshot( boundState );
        assertEquals( Optional.of( publishedRaftId ), raftId );
    }

    private void assertNoSnapshot( BoundState boundState )
    {
        assertTrue( boundState.snapshot().isEmpty() );
    }
}
