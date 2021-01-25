/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.dbms.identity.ServerId;
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
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
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
    private static final NamedDatabaseId DEFAULT_DATABASE_ID = DatabaseIdFactory.from( DEFAULT_DATABASE_NAME, randomUUID() );

    private static final StoreId SOME_STORE_ID = new StoreId( 0, 0, 0 );

    private final RaftBootstrapper raftBootstrapper = mock( RaftBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    private final Config config = Config.defaults();
    private final int minCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
    private final CoreServerIdentity me = new InMemoryCoreServerIdentity();

    private Set<UUID> extractServerUUIDs( Map<ServerId,CoreServerInfo> servers )
    {
        return servers.keySet().stream().map( ServerId::uuid ).collect( toSet() );
    }

    private ClusterSystemGraphDbmsModel systemGraphFor( NamedDatabaseId namedDatabaseId, Set<UUID> initialServers )
    {
        ClusterSystemGraphDbmsModel systemGraph = mock( ClusterSystemGraphDbmsModel.class );
        when( systemGraph.getStoreId( namedDatabaseId ) ).thenReturn( SOME_STORE_ID );
        when( systemGraph.getInitialServers( namedDatabaseId ) ).thenReturn( initialServers );
        return systemGraph;
    }

    private DatabaseStartAborter neverAbort()
    {
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( false );
        return aborter;
    }

    private DatabaseStartAborter abortAfter( int n )
    {
        var falsesS = Stream.generate( () -> false ).limit( n );
        var answers = Stream.concat( falsesS, Stream.of( true ) ).collect( Collectors.toList() );
        var head = answers.get( 0 );
        var tail = answers.subList( 1, answers.size() ).toArray(Boolean[]::new);
        var aborter = mock( DatabaseStartAborter.class );
        when( aborter.shouldAbort( any( NamedDatabaseId.class ) ) ).thenReturn( head, tail );
        return aborter;
    }

    private RaftBinder raftBinder( SimpleStorage<RaftGroupId> raftIdStorage, CoreTopologyService topologyService )
    {
        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, emptySet() );
        return new RaftBinder( SOME_NAMED_DATABASE_ID, me, raftIdStorage, topologyService, systemGraph, clock,
                               () -> clock.forward( 1, TimeUnit.SECONDS ), Duration.ofSeconds( 10 ), raftBootstrapper, minCoreHosts, new Monitors() );
    }

    private RaftBinder raftBinder( SimpleStorage<RaftGroupId> raftIdStorage, CoreTopologyService topologyService, NamedDatabaseId namedDatabaseId,
            ClusterSystemGraphDbmsModel systemGraph )
    {
        return new RaftBinder( namedDatabaseId, me, raftIdStorage, topologyService, systemGraph, clock,
                               () -> clock.forward( 1, TimeUnit.SECONDS ), Duration.ofSeconds( 10 ), raftBootstrapper, minCoreHosts, new Monitors() );
    }

    @Test
    void shouldThrowOnAbort()
    {
        // given
        var unboundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, emptyMap() );
        var topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( unboundTopology );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );
        var aborter = abortAfter( 0 );

        // when / then
        assertThrows( DatabaseStartAbortedException.class, () -> binder.bindToRaft( aborter ) );
    }

    @Test
    void shouldThrowOnRaftIdDatabaseIdMismatch()
    {
        // given
        var previouslyBoundRaftId = IdFactory.randomRaftId();

        var raftIdStorage = new InMemorySimpleStorage<RaftGroupId>();
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
        RaftGroupId publishedRaftGroupId = RaftGroupId.from( SOME_NAMED_DATABASE_ID.databaseId() );
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, emptyMap() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), publishedRaftGroupId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_NAMED_DATABASE_ID, systemGraph );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        Optional<RaftGroupId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( publishedRaftGroupId, raftId.get() );
        verify( topologyService, never() ).publishRaftId( publishedRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( SOME_NAMED_DATABASE_ID );
    }

    @Test
    void shouldPublishStoredRaftIdIfPreviouslyBound() throws Throwable
    {
        // given
        RaftGroupId previouslyBoundRaftGroupId = RaftGroupId.from( SOME_NAMED_DATABASE_ID.databaseId() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.publishRaftId( previouslyBoundRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) ) ).thenReturn(
                PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );

        InMemorySimpleStorage<RaftGroupId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftGroupId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        verify( topologyService ).publishRaftId( previouslyBoundRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) );
        Optional<RaftGroupId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( previouslyBoundRaftGroupId, raftId.get() );
    }

    @Test
    void shouldRetryWhenPublishOfStoredRaftIdFailsWithTransientErrors() throws Throwable
    {
        // given
        RaftGroupId previouslyBoundRaftGroupId = RaftGroupId.from( SOME_NAMED_DATABASE_ID.databaseId() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.publishRaftId( previouslyBoundRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) ) )
                .thenReturn( PublishRaftIdOutcome.MAYBE_PUBLISHED )
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );

        InMemorySimpleStorage<RaftGroupId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftGroupId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        verify( topologyService, atLeast( 2 ) ).publishRaftId( previouslyBoundRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) );
    }

    @Test
    void shouldNotRetryWhenOtherMemberPublishesStoredRaftId() throws Throwable
    {
        // given
        RaftGroupId previouslyBoundRaftGroupId = RaftGroupId.from( SOME_NAMED_DATABASE_ID.databaseId() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.publishRaftId( previouslyBoundRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) ) )
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_OTHER );

        InMemorySimpleStorage<RaftGroupId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftGroupId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        assertNoSnapshot( boundState );
        verify( topologyService, times( 1 ) ).publishRaftId( previouslyBoundRaftGroupId, me.raftMemberId( SOME_NAMED_DATABASE_ID ) );
    }

    @Test
    void shouldRetryWhenPublishFailsWithTransientErrors() throws Throwable
    {
        // given
        var namedDatabaseId = randomNamedDatabaseId();
        var servers = IntStream.range( 0, minCoreHosts ).boxed().collect( Collectors.toMap(
                        i -> IdFactory.randomServerId(),
                        i -> addressesForCore( i, singleton( namedDatabaseId.databaseId() ) ) ) );
        var raftMembers = servers.keySet().stream().map( sid -> new RaftMemberId( sid.uuid() ) ).collect( Collectors.toSet() );

        var bootstrappableTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), null, servers );
        var topologyService = mock( CoreTopologyService.class );

        when( topologyService.publishRaftId( any( RaftGroupId.class ), any( RaftMemberId.class ) ) )
                .thenReturn( PublishRaftIdOutcome.MAYBE_PUBLISHED ) // Cause first retry
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_OTHER ) // Cause second retry
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME ); // Finally succeed
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( bootstrappableTopology );
        when( topologyService.canBootstrapDatabase( namedDatabaseId ) ).thenReturn( true );
        servers.keySet().forEach( server ->
                when( topologyService.resolveRaftMemberForServer( namedDatabaseId, server )).thenReturn( new RaftMemberId( server.uuid() ) ) );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers ) ).thenReturn( snapshot );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, namedDatabaseId, systemGraphFor( namedDatabaseId, emptySet() ) );

        // when
        binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService, atLeast( 3 ) ).publishRaftId( RaftGroupId.from( namedDatabaseId.databaseId() ), me.raftMemberId( namedDatabaseId ) );
    }

    @Test
    void shouldTimeoutIfPublishRaftIdContinuallyFailsWithTransientErrors() throws Throwable
    {
        // given
        var namedDatabaseId = randomNamedDatabaseId();
        var servers = IntStream.range( 0, minCoreHosts ).boxed().collect( Collectors.toMap(
                i -> IdFactory.randomServerId(),
                i -> addressesForCore( i, singleton( namedDatabaseId.databaseId() ) ) ) );

        var bootstrappableTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), null, servers );
        var topologyService = mock( CoreTopologyService.class );
        var raftMembers = servers.keySet().stream().map( sid -> new RaftMemberId( sid.uuid() ) ).collect( Collectors.toSet() );

        when( topologyService.publishRaftId( any( RaftGroupId.class ), any( RaftMemberId.class ) ) ).thenReturn( PublishRaftIdOutcome.MAYBE_PUBLISHED );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( bootstrappableTopology );
        when( topologyService.canBootstrapDatabase( namedDatabaseId ) ).thenReturn( true );
        servers.keySet().forEach( server ->
                when( topologyService.resolveRaftMemberForServer( namedDatabaseId, server )).thenReturn( new RaftMemberId( server.uuid() ) ) );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers ) ).thenReturn( snapshot );

        var binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, namedDatabaseId, systemGraphFor( namedDatabaseId, emptySet() ) );

        // when / then
        assertThrows( TimeoutException.class, () -> binder.bindToRaft( neverAbort() ) );
        verify( topologyService, atLeast( 1 ) ).publishRaftId( RaftGroupId.from( namedDatabaseId.databaseId() ), me.raftMemberId( namedDatabaseId ) );
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
        var servers = IntStream.range( 0, minCoreHosts ).boxed()
                .collect( Collectors.toMap( i -> IdFactory.randomServerId(), i -> addressesForCore( i ) ) );
        var raftMembers = servers.keySet().stream().map( sId -> new RaftMemberId( sId.uuid() ) ).collect( Collectors.toSet() );

        DatabaseCoreTopology topology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), null, servers );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) ).thenReturn( topology );
        when( topologyService.publishRaftId( any(), any() ) ).thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );
        when( topologyService.canBootstrapDatabase( namedDatabaseId ) ).thenReturn( true );
        servers.keySet().forEach( serverId -> when( topologyService.resolveRaftMemberForServer( namedDatabaseId, serverId ) ).thenReturn(
                new RaftMemberId( serverId.uuid() ) ) );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers ) ).thenReturn( snapshot );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( namedDatabaseId, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, namedDatabaseId, systemGraph );

        // when
        BoundState boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( raftBootstrapper ).bootstrap( raftMembers );
        Optional<RaftGroupId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), namedDatabaseId.databaseId().uuid() );
        verify( topologyService ).publishRaftId( raftId.get(), me.raftMemberId( namedDatabaseId ) );
        assertHasSnapshot( boundState );
        assertEquals( snapshot, boundState.snapshot().orElseThrow() );
    }

    private void assertHasSnapshot( BoundState boundState )
    {
        assertTrue( boundState.snapshot().isPresent() );
    }

    @Test
    void shouldBootstrapNonInitialDatabaseUsingSystemDatabaseMethod() throws Throwable
    {
        // given
        var servers = IntStream.range( 0, minCoreHosts - 1 )
                .mapToObj( i -> Pair.of( IdFactory.randomServerId(), addressesForCore( i ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
        servers.put( me.serverId(), addressesForCore( minCoreHosts ) );
        var raftMembers = servers.keySet().stream().map( sid -> new RaftMemberId( sid.uuid() ) ).collect( Collectors.toSet() );

        DatabaseCoreTopology topology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), null, servers );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( topology );
        when( topologyService.publishRaftId( any(), any() ) ).thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );
        when( topologyService.canBootstrapDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( true );
        servers.keySet().forEach( serverId -> when( topologyService.resolveRaftMemberForServer( SOME_NAMED_DATABASE_ID, serverId ) )
                .thenReturn( new RaftMemberId( serverId.uuid() ) ) );

        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( raftMembers, SOME_STORE_ID ) ).thenReturn( snapshot );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, extractServerUUIDs( servers ) );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_NAMED_DATABASE_ID, systemGraph );

        // when
        BoundState boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( raftBootstrapper ).bootstrap( raftMembers, SOME_STORE_ID );
        Optional<RaftGroupId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), SOME_NAMED_DATABASE_ID.databaseId().uuid() );
        verify( topologyService ).publishRaftId( raftId.get(), me.raftMemberId( SOME_NAMED_DATABASE_ID ) );
        assertHasSnapshot( boundState );
        assertEquals( snapshot, boundState.snapshot().orElseThrow() );
    }

    @Test
    void shouldNotBootstrapDatabaseWhenRaftIdAlreadyPublished() throws Exception
    {
        // given
        RaftGroupId publishedRaftGroupId = RaftGroupId.from( SOME_NAMED_DATABASE_ID.databaseId() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( SOME_NAMED_DATABASE_ID.databaseId(), publishedRaftGroupId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( SOME_NAMED_DATABASE_ID ) ).thenReturn( boundTopology );

        ClusterSystemGraphDbmsModel systemGraph = systemGraphFor( SOME_NAMED_DATABASE_ID, emptySet() );
        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService, SOME_NAMED_DATABASE_ID, systemGraph );

        // when
        var boundState = binder.bindToRaft( neverAbort() );

        // then
        verify( topologyService ).coreTopologyForDatabase( SOME_NAMED_DATABASE_ID );
        verifyNoInteractions( raftBootstrapper );

        Optional<RaftGroupId> raftId = binder.get();
        assertEquals( Optional.of( publishedRaftGroupId ), raftId );
        assertNoSnapshot( boundState );
    }

    @Test
    void shouldNotBootstrapSystemDatabaseWhenRaftIdAlreadyPublished() throws Exception
    {
        // given
        RaftGroupId publishedRaftGroupId = RaftGroupId.from( NAMED_SYSTEM_DATABASE_ID.databaseId() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( NAMED_SYSTEM_DATABASE_ID.databaseId(), publishedRaftGroupId, emptyMap() );

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

        Optional<RaftGroupId> raftId = binder.get();
        assertNoSnapshot( boundState );
        assertEquals( Optional.of( publishedRaftGroupId ), raftId );
    }

    @ParameterizedTest( name = "{1}" )
    @MethodSource( "initialDatabases" )
    void shouldEventuallyBootstrapSystemDatabaseWhenRaftIdInitiallyFailsToPublish( NamedDatabaseId namedDatabaseId,
                                                                                   @SuppressWarnings( "unused" ) String databaseName ) throws Exception
    {
        // given
        var topologyMembers = IntStream.range( 0, minCoreHosts ).boxed()
                .collect( Collectors.toMap( i -> new ServerId( randomUUID() ), i -> addressesForCore( i ) ) );
        var raftId = RaftGroupId.from( namedDatabaseId.databaseId() );
        var boundTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), raftId, topologyMembers );

        var topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) )
                .thenReturn( boundTopology );
        when( topologyService.publishRaftId( eq( raftId ), any( RaftMemberId.class ) ) )
                .thenReturn( PublishRaftIdOutcome.MAYBE_PUBLISHED )
                .thenReturn( PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME );
        when( topologyService.canBootstrapDatabase( namedDatabaseId ) ).thenReturn( true );
        when( topologyService.didBootstrapDatabase( namedDatabaseId ) ).thenReturn( true );

        var systemGraph = systemGraphFor( namedDatabaseId, emptySet() );
        var binder = raftBinder( new InMemorySimpleStorage<>(),
                topologyService, namedDatabaseId, systemGraph );
        var aborter = abortAfter( 3 );

        var snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( anySet() ) ).thenReturn( snapshot );

        // when
        var boundState = binder.bindToRaft( aborter );

        // then
        verify( topologyService, atLeast( 2 ) ).publishRaftId( eq( raftId ), any( RaftMemberId.class ) );
        assertHasSnapshot( boundState );
    }

    private void assertNoSnapshot( BoundState boundState )
    {
        assertTrue( boundState.snapshot().isEmpty() );
    }
}
