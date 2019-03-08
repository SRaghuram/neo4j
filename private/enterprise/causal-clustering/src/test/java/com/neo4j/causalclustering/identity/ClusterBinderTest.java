/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.hazelcast.core.OperationTimeoutException;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.state.CoreBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.TestTopology;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ClusterBinderTest
{
    private final CoreBootstrapper coreBootstrapper = mock( CoreBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    private final Config config = Config.defaults();
    private final int minCoreHosts = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
    private final String dbName = config.get( CausalClusteringSettings.database );

    private ClusterBinder clusterBinder( SimpleStorage<ClusterId> clusterIdStorage,
            CoreTopologyService topologyService )
    {
        return new ClusterBinder( clusterIdStorage, new StubSimpleStorage<>(), topologyService, clock, () -> clock.forward( 1, TimeUnit.SECONDS ),
                Duration.of( 3_000, MILLIS ), coreBootstrapper, dbName, minCoreHosts, new Monitors() );
    }

    @Test
    public void shouldRetryWhenPublishFailsWithTransientErrors() throws Throwable
    {
        // given
        Map<MemberId,CoreServerInfo> members = IntStream.range(0, minCoreHosts)
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        CoreTopology bootstrappableTopology = new CoreTopology( null, true, members );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );

        when( topologyService.setClusterId( any(), anyString() ) )
                .thenThrow( OperationTimeoutException.class ) // Cause a retry one
                .thenReturn( true ); // Then succeed
        when( topologyService.localCoreServers() ).thenReturn( bootstrappableTopology );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        // when
        binder.bindToCluster();

        // then
        verify( topologyService, atLeast( 2 ) ).setClusterId( any(), anyString() );
    }

    @Test( expected = TimeoutException.class )
    public void shouldTimeoutIfPublishContinuallyFailsWithTransientErrors() throws Throwable
    {
        // given
        Map<MemberId,CoreServerInfo> members = IntStream.range(0, minCoreHosts)
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        CoreTopology bootstrappableTopology = new CoreTopology( null, true, members );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );

        when( topologyService.setClusterId( any(), anyString() ) )
                .thenThrow( OperationTimeoutException.class ); // Causes a retry
        when( topologyService.localCoreServers() ).thenReturn( bootstrappableTopology );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        // when
        binder.bindToCluster();
    }

    @Test
    public void shouldTimeoutWhenNotBootstrappableAndNobodyElsePublishesClusterId() throws Throwable
    {
        // given
        CoreTopology unboundTopology = new CoreTopology( null, false, emptyMap() );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.localCoreServers() ).thenReturn( unboundTopology );

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
        verify( topologyService, atLeast( 2 ) ).localCoreServers();
    }

    @Test
    public void shouldBindToClusterIdPublishedByAnotherMember() throws Throwable
    {
        // given
        ClusterId publishedClusterId = new ClusterId( UUID.randomUUID() );
        CoreTopology unboundTopology = new CoreTopology( null, false, emptyMap() );
        CoreTopology boundTopology = new CoreTopology( publishedClusterId, false, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.localCoreServers() ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        // when
        binder.bindToCluster();

        // then
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        assertEquals( publishedClusterId, clusterId.get() );
        verify( topologyService, atLeast( 2 ) ).localCoreServers();
    }

    @Test
    public void shouldPublishStoredClusterIdIfPreviouslyBound() throws Throwable
    {
        // given
        ClusterId previouslyBoundClusterId = new ClusterId( UUID.randomUUID() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setClusterId( previouslyBoundClusterId, "default" ) ).thenReturn( true );

        StubSimpleStorage<ClusterId> clusterIdStorage = new StubSimpleStorage<>();
        clusterIdStorage.writeState( previouslyBoundClusterId );

        ClusterBinder binder = clusterBinder( clusterIdStorage, topologyService );

        // when
        binder.bindToCluster();

        // then
        verify( topologyService ).setClusterId( previouslyBoundClusterId, "default" );
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        assertEquals( previouslyBoundClusterId, clusterId.get() );
    }

    @Test
    public void shouldFailToPublishMismatchingStoredClusterId() throws Throwable
    {
        // given
        ClusterId previouslyBoundClusterId = new ClusterId( UUID.randomUUID() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setClusterId( previouslyBoundClusterId, "default" ) ).thenReturn( false );

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
    public void shouldBootstrapWhenBootstrappable() throws Throwable
    {
        // given
        String databaseName = DEFAULT_DATABASE_NAME;
        Map<MemberId,CoreServerInfo> members = IntStream.range(0, minCoreHosts)
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        CoreTopology bootstrappableTopology = new CoreTopology( null, true, members );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.localCoreServers() ).thenReturn( bootstrappableTopology );
        when( topologyService.setClusterId( any(), eq("default" ) ) ).thenReturn( true );
        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( coreBootstrapper.bootstrap( any() ) ).thenReturn( singletonMap( databaseName, snapshot ) );

        ClusterBinder binder = clusterBinder( new StubSimpleStorage<>(), topologyService );

        // when
        BoundState boundState = binder.bindToCluster();

        // then
        verify( coreBootstrapper ).bootstrap( any() );
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        verify( topologyService ).setClusterId( clusterId.get(), "default" );
        assertEquals( singletonMap( databaseName, snapshot ), boundState.snapshots() );
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
