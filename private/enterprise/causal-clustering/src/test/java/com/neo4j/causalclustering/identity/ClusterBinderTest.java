/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.state.CoreBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
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
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusterBinderTest
{
    private final CoreBootstrapper coreBootstrapper = mock( CoreBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    private final Config config = Config.defaults();
    private final int minCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
    private final DatabaseId databaseId = new DatabaseId( "my_database" );

    private ClusterBinder clusterBinder( SimpleStorage<ClusterId> clusterIdStorage,
            CoreTopologyService topologyService )
    {
        return new ClusterBinder( databaseId, clusterIdStorage, topologyService, clock, () -> clock.forward( 1, TimeUnit.SECONDS ),
                Duration.of( 3_000, MILLIS ), coreBootstrapper, minCoreHosts, new Monitors() );
    }

    @Test
    void shouldTimeoutWhenNotBootstrappableAndNobodyElsePublishesClusterId() throws Throwable
    {
        // given
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( databaseId, null, emptyMap() );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( unboundTopology );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        try
        {
            // when
            binder.bindToCluster();
            fail( "Should have timed out" );
        }
        catch ( TimeoutException e )
        {
            // expected
        }

        // then
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( databaseId );
    }

    @Test
    void shouldBindToClusterIdPublishedByAnotherMember() throws Throwable
    {
        // given
        ClusterId publishedClusterId = new ClusterId( UUID.randomUUID() );
        DatabaseCoreTopology unboundTopology = new DatabaseCoreTopology( databaseId, null, emptyMap() );
        DatabaseCoreTopology boundTopology = new DatabaseCoreTopology( databaseId, publishedClusterId, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        // when
        binder.bindToCluster();

        // then
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        assertEquals( publishedClusterId, clusterId.get() );
        verify( topologyService, atLeast( 2 ) ).coreTopologyForDatabase( databaseId );
    }

    @Test
    void shouldPublishStoredClusterIdIfPreviouslyBound() throws Throwable
    {
        // given
        ClusterId previouslyBoundClusterId = new ClusterId( UUID.randomUUID() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setClusterId( previouslyBoundClusterId, databaseId ) ).thenReturn( true );

        StubSimpleStorage<ClusterId> clusterIdStorage = new StubSimpleStorage<>();
        clusterIdStorage.writeState( previouslyBoundClusterId );

        ClusterBinder binder = clusterBinder( clusterIdStorage, topologyService );

        // when
        binder.bindToCluster();

        // then
        verify( topologyService ).setClusterId( previouslyBoundClusterId, databaseId );
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        assertEquals( previouslyBoundClusterId, clusterId.get() );
    }

    @Test
    void shouldFailToPublishMismatchingStoredClusterId() throws Throwable
    {
        // given
        ClusterId previouslyBoundClusterId = new ClusterId( UUID.randomUUID() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setClusterId( previouslyBoundClusterId, databaseId ) ).thenReturn( false );

        StubSimpleStorage<ClusterId> clusterIdStorage = new StubSimpleStorage<>();
        clusterIdStorage.writeState( previouslyBoundClusterId );

        ClusterBinder binder = clusterBinder( clusterIdStorage, topologyService );

        // when
        try
        {
            binder.bindToCluster();
            fail( "Should have thrown exception" );
        }
        catch ( BindingException e )
        {
            // expected
        }
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
        when( topologyService.setClusterId( any(), eq( databaseId ) ) ).thenReturn( true );
        when( topologyService.canBootstrapCluster( databaseId ) ).thenReturn( true );
        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( coreBootstrapper.bootstrap( any() ) ).thenReturn( singletonMap( databaseId, snapshot ) );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        // when
        BoundState boundState = binder.bindToCluster();

        // then
        verify( coreBootstrapper ).bootstrap( any() );
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        verify( topologyService ).setClusterId( clusterId.get(), databaseId );
        assertEquals( singletonMap( databaseId, snapshot ), boundState.snapshots() );
    }

    private class StubSimpleStorage<T> implements SimpleStorage<T>
    {
        private T state;

        @Override
        public boolean exists()
        {
            return state != null;
        }

        @Override
        public T readState()
        {
            return state;
        }

        @Override
        public void writeState( T state )
        {
            this.state = state;
        }
    }
}
