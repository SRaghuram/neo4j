/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.Role;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_routing_ttl;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static com.neo4j.causalclustering.discovery.TestTopology.readReplicaInfoMap;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.values.storable.Values.longValue;

@RunWith( Parameterized.class )
public class GetServersProcedureV1Test
{
    private final ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Parameterized.Parameter( 0 )
    public String description;
    @Parameterized.Parameter( 1 )
    public Config config;
    @Parameterized.Parameter( 2 )
    public boolean expectFollowersAsReadEndPoints;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> params()
    {
        return Arrays.asList(
                new Object[]{"with followers as read end points", Config.defaults(
                        cluster_allow_reads_on_followers, Settings.TRUE ), true },
                new Object[]{"no followers as read end points", Config.defaults(
                        cluster_allow_reads_on_followers, Settings.FALSE ), false }
        );
    }

    @Test
    public void ttlShouldBeInSeconds() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, new HashMap<>() );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        // set the TTL in minutes
        config.augment( cluster_routing_ttl, "10m" );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        List<AnyValue[]> results = asList( proc.apply( null, new AnyValue[0], null ) );

        // then
        AnyValue[] rows = results.get( 0 );
        LongValue ttlInSeconds = (LongValue) rows[0];
        assertEquals( longValue( 600 ), ttlInSeconds );
    }

    @Test
    public void shouldHaveCorrectSignature()
    {
        // given
        final LegacyGetServersProcedure proc = new LegacyGetServersProcedure( null, null, config, getInstance() );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.outputSignature(), containsInAnyOrder(
                FieldSignature.outputField( "ttl", NTInteger ),
                FieldSignature.outputField( "servers", NTList( NTMap ) ) ) );
    }

    @Test
    public void shouldProvideReaderAndRouterForSingleCoreSetup() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );
        coreMembers.put( member( 1 ), addressesForCore( 1, false ) );
        coreMembers.put( member( 2 ), addressesForCore( 2, false ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.readAddress( addressesForCore( 1, false ).connectors().boltAddress() );
        builder.readAddress( addressesForCore( 2, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 1, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 2, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnTheCoreLeaderForWriteAndReadReplicasAndCoresForReads() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, addressesForCore( 0, false ) );

        when( topologyService.localCoreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( readReplicaInfoMap( 1 ) ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        if ( expectFollowersAsReadEndPoints )
        {
            builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        }
        builder.readAddress( addressesForReadReplica( 1 ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnCoreMemberAsReadServerIfNoReadReplicasAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, addressesForCore( 0, false ) );

        when( topologyService.localCoreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        when( topologyService.localCoreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        when( topologyService.localCoreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then

        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @SuppressWarnings( "unchecked" )
    private ClusterView run( LegacyGetServersProcedure proc ) throws ProcedureException
    {
        final AnyValue[] rows = asList( proc.apply( null, new AnyValue[0], null ) ).get( 0 );
        assertEquals( longValue( config.get( cluster_routing_ttl ).getSeconds() ), /* ttl */(LongValue) rows[0] );
        return ClusterView.parse( (ListValue) rows[1] );
    }

    private static class ClusterView
    {
        private final Map<Role,Set<AdvertisedSocketAddress>> clusterView;

        private ClusterView( Map<Role,Set<AdvertisedSocketAddress>> clusterView )
        {
            this.clusterView = clusterView;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ClusterView that = (ClusterView) o;
            return Objects.equals( clusterView, that.clusterView );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( clusterView );
        }

        @Override
        public String toString()
        {
            return "ClusterView{" + "clusterView=" + clusterView + '}';
        }

        static ClusterView parse( ListValue result )
        {
            Map<Role,Set<AdvertisedSocketAddress>> view = new HashMap<>();
            for ( AnyValue value : result )
            {
                MapValue single = (MapValue) value;
                Role role = Role.valueOf( ((TextValue) single.get( "role" )).stringValue() );
                Set<AdvertisedSocketAddress> addresses = parseAdresses( (ListValue) single.get( "addresses" ) );
                assertFalse( view.containsKey( role ) );
                view.put( role, addresses );
            }

            return new ClusterView( view );
        }

        private static Set<AdvertisedSocketAddress> parseAdresses( ListValue addresses )
        {

            List<AdvertisedSocketAddress> list = new ArrayList<>( addresses.size() );
            for ( AnyValue address : addresses )
            {
                list.add( parse( ((TextValue) address).stringValue() ) );
            }
            Set<AdvertisedSocketAddress> set = new HashSet<>( list );
            assertEquals( list.size(), set.size() );
            return set;
        }

        private static AdvertisedSocketAddress parse( String address )
        {
            String[] split = address.split( ":" );
            assertEquals( 2, split.length );
            return new AdvertisedSocketAddress( split[0], Integer.valueOf( split[1] ) );
        }

        static class Builder
        {
            private final Map<Role,Set<AdvertisedSocketAddress>> view = new HashMap<>();

            Builder readAddress( AdvertisedSocketAddress address )
            {
                addAddress( Role.READ, address );
                return this;
            }

            Builder writeAddress( AdvertisedSocketAddress address )
            {
                addAddress( Role.WRITE, address );
                return this;
            }

            Builder routeAddress( AdvertisedSocketAddress address )
            {
                addAddress( Role.ROUTE, address );
                return this;
            }

            private void addAddress( Role role, AdvertisedSocketAddress address )
            {
                Set<AdvertisedSocketAddress> advertisedSocketAddresses = view.computeIfAbsent( role, k -> new HashSet<>() );
                advertisedSocketAddresses.add( address );
            }

            public ClusterView build()
            {
                return new ClusterView( view );
            }
        }
    }
}
