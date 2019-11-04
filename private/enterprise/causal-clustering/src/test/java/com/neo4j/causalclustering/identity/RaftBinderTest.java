/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.state.RaftBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.storage.InMemorySimpleStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
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
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

class RaftBinderTest
{
    private static final DatabaseId SOME_DATABASE_ID = TestDatabaseIdRepository.randomDatabaseId();
    private static final DatabaseId DEFAULT_DATABASE_ID = DatabaseIdFactory.from( DEFAULT_DATABASE_NAME, UUID.randomUUID() );

    private static final StoreId SOME_STORE_ID = new StoreId( 0, 0, 0 );

    private final RaftBootstrapper raftBootstrapper = mock( RaftBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    private final Config config = Config.defaults();
    private final int minCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
    private final MemberId myIdentity = new MemberId( randomUUID() );

    private Set<UUID> extractMemberUUIDs( Map<MemberId,CoreServerInfo> members )
    {
        return members.keySet().stream().map( MemberId::getUuid ).collect( toSet() );
    }

    private ClusterSystemGraphDbmsModel systemGraphFor( DatabaseId databaseId, Set<UUID> initialMembers )
    {
        ClusterSystemGraphDbmsModel systemGraph = mock( ClusterSystemGraphDbmsModel.class );
        when( systemGraph.getStoreId( databaseId ) ).thenReturn( SOME_STORE_ID );
        when( systemGraph.getInitialMembers( databaseId ) ).thenReturn( initialMembers );
        return systemGraph;
    }

    private DatabaseStartAborter neverAbort()
    {
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( DatabaseId.class ) ) ).thenReturn( false );
        return aborter;
    }

    private RaftBinder raftBinder( SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService )
    {
        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_DATABASE_ID, emptySet() );
        return new RaftBinder( SOME_DATABASE_ID, myIdentity, raftIdStorage, topologyService, systemGraph, clock, () -> clock.forward( 1, TimeUnit.SECONDS ),
                Duration.of( 3_000, MILLIS ), raftBootstrapper, minCoreHosts, new Monitors(), nullDatabaseLogProvider() );
    }

    private RaftBinder raftBinder( SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService, DatabaseId databaseId,
            ClusterSystemGraphDbmsModel systemGraph )
    {
        return new RaftBinder( databaseId, myIdentity, raftIdStorage, topologyService, systemGraph, clock, () -> clock.forward( 1, TimeUnit.SECONDS ),
                Duration.of( 3_000, MILLIS ), raftBootstrapper, minCoreHosts, new Monitors(), nullDatabaseLogProvider() );
    }

    @Test
    void shouldThrowOnAbort()
    {
        // given
        var unboundTopology = new DatabaseCoreTopology( SOME_DATABASE_ID, null, emptyMap() );
        var topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_DATABASE_ID ) ).thenReturn( unboundTopology );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( DatabaseId.class ) ) ).thenReturn( true );

        // when / then
        assertThrows( DatabaseStartAbortedException.class, () -> binder.bindToRaft( aborter ) );
    }

    @Test
    void shouldThrowOnRaftIdDatabaseIdMismatch()
    {
        // given
        var previouslyBoundRaftId = RaftIdFactory.random();

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
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( SOME_DATABASE_ID, null, emptyMap() );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_DATABASE_ID ) ).thenReturn( unboundTopology );

        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );

        // when / then
        assertThrows( TimeoutException.class, () -> binder.bindToRaft( neverAbort() ) );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( SOME_DATABASE_ID );
    }

    @Test
    void shouldBindToRaftIdPublishedByAnotherMember() throws Throwable
    {
        // given
        RaftId publishedRaftId = RaftId.from( SOME_DATABASE_ID );
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( SOME_DATABASE_ID, null, emptyMap() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SOME_DATABASE_ID, publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_DATABASE_ID ) ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_DATABASE_ID, systemGraph );

        // when
        binder.bindToRaft( neverAbort() );

        // then
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( publishedRaftId, raftId.get() );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( SOME_DATABASE_ID );
    }

    @Test
    void shouldPublishStoredRaftIdIfPreviouslyBound() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( SOME_DATABASE_ID );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setRaftId( previouslyBoundRaftId ) ).thenReturn( true );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService ).setRaftId( previouslyBoundRaftId );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( previouslyBoundRaftId, raftId.get() );
    }

    @Test
    void shouldThrowWhenFailsToPublishStoredRaftId() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( SOME_DATABASE_ID );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setRaftId( previouslyBoundRaftId ) ).thenReturn( false );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when / then
        assertThrows( BindingException.class, () -> binder.bindToRaft( neverAbort() ) );
    }

    static Stream<Arguments> initialDatabases()
    {
        return Stream.of(
                Arguments.of( SYSTEM_DATABASE_ID, "system" ),
                Arguments.of( DEFAULT_DATABASE_ID, "neo4j" ),
                Arguments.of( DatabaseIdFactory.from( "other_default", UUID.randomUUID() ), "other_default" )
        );
    }

    @ParameterizedTest( name = "{1}" )
    @MethodSource( "initialDatabases" )
    void shouldBootstrapInitialDatabasesUsingDiscoveryMethod( DatabaseId databaseId, @SuppressWarnings( "unused" ) String databaseName ) throws Throwable
    {
        // given
        Map<MemberId,CoreServerInfo> topologyMembers = IntStream.range( 0, minCoreHosts )
                .mapToObj( i -> Pair.of( new MemberId( randomUUID() ), addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        DatabaseCoreTopology topology = new DatabaseCoreTopology( databaseId, null, topologyMembers );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( topology );
        when( topologyService.setRaftId( any() ) ).thenReturn( true );
        when( topologyService.canBootstrapRaftGroup( databaseId ) ).thenReturn( true );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( topologyMembers.keySet() ) ).thenReturn( snapshot );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( databaseId, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, databaseId, systemGraph );

        // when
        BoundState boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( raftBootstrapper ).bootstrap( topologyMembers.keySet() );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), databaseId.uuid() );
        verify( topologyService ).setRaftId( raftId.get() );
        assertTrue( boundState.snapshot().isPresent() );
        assertEquals( snapshot, boundState.snapshot().get() );
    }

    @Test
    void shouldBootstrapNonInitialDatabaseUsingSystemDatabaseMethod() throws Throwable
    {
        // given
        Map<MemberId,CoreServerInfo> topologyMembers = IntStream.range( 0, minCoreHosts - 1 )
                .mapToObj( i -> Pair.of( new MemberId( randomUUID() ), addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        topologyMembers.put( myIdentity, addressesForCore( minCoreHosts, false ) );

        DatabaseCoreTopology topology = new DatabaseCoreTopology( SOME_DATABASE_ID, null, topologyMembers );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_DATABASE_ID ) ).thenReturn( topology );
        when( topologyService.setRaftId( any() ) ).thenReturn( true );
        when( topologyService.canBootstrapRaftGroup( SOME_DATABASE_ID ) ).thenReturn( true );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( topologyMembers.keySet(), SOME_STORE_ID ) ).thenReturn( snapshot );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_DATABASE_ID, extractMemberUUIDs( topologyMembers ) );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_DATABASE_ID, systemGraph );

        // when
        BoundState boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( raftBootstrapper ).bootstrap( topologyMembers.keySet(), SOME_STORE_ID );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), SOME_DATABASE_ID.uuid() );
        verify( topologyService ).setRaftId( raftId.get() );
        assertTrue( boundState.snapshot().isPresent() );
        assertEquals( snapshot, boundState.snapshot().get() );
    }

    @Test
    void shouldNotBootstrapDatabaseWhenRaftIdAlreadyPublished() throws Exception
    {
        // given
        RaftId publishedRaftId = RaftId.from( SOME_DATABASE_ID );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SOME_DATABASE_ID, publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_DATABASE_ID ) ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_DATABASE_ID, systemGraph );

        // when
        binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService ).coreTopologyForDatabase( SOME_DATABASE_ID );
        verifyZeroInteractions( raftBootstrapper );

        Optional<RaftId> raftId = binder.get();
        assertEquals( Optional.of( publishedRaftId ), raftId );
    }

    @Test
    void shouldNotBootstrapSystemDatabaseWhenRaftIdAlreadyPublished() throws Exception
    {
        // given
        RaftId publishedRaftId = RaftId.from( SYSTEM_DATABASE_ID );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SYSTEM_DATABASE_ID, publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SYSTEM_DATABASE_ID ) ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SYSTEM_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SYSTEM_DATABASE_ID, systemGraph );

        // when
        binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService ).coreTopologyForDatabase( SYSTEM_DATABASE_ID );
        verify( raftBootstrapper ).removeStore();
        verifyNoMoreInteractions( raftBootstrapper );

        Optional<RaftId> raftId = binder.get();
        assertEquals( Optional.of( publishedRaftId ), raftId );
    }
}
