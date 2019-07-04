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
import com.neo4j.causalclustering.discovery.TestTopology;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RaftBinderTest
{
    private final RaftBootstrapper raftBootstrapper = mock( RaftBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    private final Config config = Config.defaults();
    private final int minCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
    private final DatabaseId databaseId = TestDatabaseIdRepository.randomDatabaseId();

    private RaftBinder raftBinder( SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService )
    {
        return new RaftBinder( databaseId, raftIdStorage, topologyService, clock, () -> clock.forward( 1, TimeUnit.SECONDS ),
                Duration.of( 3_000, MILLIS ), raftBootstrapper, minCoreHosts, new Monitors() );
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
        assertThrows( exception, binder::bindToRaft );
    }

    @Test
    void shouldTimeoutWhenNotBootstrappableAndNobodyElsePublishesRaftId() throws Throwable
    {
        // given
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( databaseId, null, emptyMap() );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( unboundTopology );

        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );

        // when / then
        assertThrows( TimeoutException.class, binder::bindToRaft );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( databaseId );
    }

    @Test
    void shouldBindToRaftIdPublishedByAnotherMember() throws Throwable
    {
        // given
        RaftId publishedRaftId = RaftId.from( databaseId );
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( databaseId, null, emptyMap() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( databaseId, publishedRaftId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );

        // when
        binder.bindToRaft();

        // then
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( publishedRaftId, raftId.get() );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( databaseId );
    }

    @Test
    void shouldPublishStoredRaftIdIfPreviouslyBound() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( databaseId );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setRaftId( previouslyBoundRaftId, databaseId ) ).thenReturn( true );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when
        binder.bindToRaft();

        // then
        verify( topologyService ).setRaftId( previouslyBoundRaftId, databaseId );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( previouslyBoundRaftId, raftId.get() );
    }

    @Test
    void shouldThrowWhenFailsToPublishStoredRaftId() throws Throwable
    {
        // given
        RaftId previouslyBoundRaftId = RaftId.from( databaseId );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setRaftId( previouslyBoundRaftId, databaseId ) ).thenReturn( false );

        InMemorySimpleStorage<RaftId> raftIdStorage = new InMemorySimpleStorage<>();
        raftIdStorage.writeState( previouslyBoundRaftId );

        RaftBinder binder = raftBinder( raftIdStorage, topologyService );

        // when / then
        assertThrows( BindingException.class, binder::bindToRaft );
    }

    @Test
    void shouldBootstrapWhenBootstrappable() throws Throwable
    {
        // given
        Map<MemberId,CoreServerInfo> members = IntStream.range(0, minCoreHosts)
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        DatabaseCoreTopology bootstrappableTopology = new DatabaseCoreTopology( databaseId, null, members );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( bootstrappableTopology );
        when( topologyService.setRaftId( any(), eq( databaseId ) ) ).thenReturn( true );
        when( topologyService.canBootstrapRaftGroup( databaseId ) ).thenReturn( true );
        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( raftBootstrapper.bootstrap( any() ) ).thenReturn( snapshot );

        RaftBinder binder = raftBinder( new InMemorySimpleStorage<>(), topologyService );

        // when
        BoundState boundState = binder.bindToRaft();

        // then
        verify( raftBootstrapper ).bootstrap( any() );
        Optional<RaftId> raftId = binder.get();
        assertTrue( raftId.isPresent() );
        assertEquals( raftId.get().uuid(), databaseId.uuid() );
        verify( topologyService ).setRaftId( raftId.get(), databaseId );
        assertTrue( boundState.snapshot().isPresent() );
        assertEquals( snapshot, boundState.snapshot().get() );
    }
}
