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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.Settings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static com.neo4j.causalclustering.discovery.TestTopology.readReplicaInfoMap;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;
import static org.neo4j.values.storable.Values.longValue;

class GetServersProcedureForSingleDCTest
{
    private final ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    @ParameterizedTest( name = "{0}" )
    @MethodSource( "routingConfigs" )
    private @interface RoutingConfigsTest
    {
    }

    private static Stream<Arguments> routingConfigs()
    {
        return Stream.of(
                Arguments.of( Config.defaults( cluster_allow_reads_on_followers, Settings.TRUE ) ),
                Arguments.of( Config.defaults( cluster_allow_reads_on_followers, Settings.FALSE ) )
        );
    }

    @RoutingConfigsTest
    void ttlShouldBeInSeconds( Config config ) throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, new HashMap<>() );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        // set the TTL in minutes
        config.augment( routing_ttl, "10m" );

        CallableProcedure proc = newProcedure( coreTopologyService, leaderLocator, config );

        // when
        List<AnyValue[]> results = asList( proc.apply( null, new AnyValue[0], null ) );

        // then
        AnyValue[] rows = results.get( 0 );
        LongValue ttlInSeconds = (LongValue) rows[0];
        assertEquals( longValue( 600 ), ttlInSeconds );
    }

    @RoutingConfigsTest
    void shouldHaveCorrectSignature( Config config )
    {
        // given
        CallableProcedure proc = newProcedure( null, null, config );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.outputSignature(), containsInAnyOrder(
                FieldSignature.outputField( "ttl", NTInteger ),
                FieldSignature.outputField( "servers", NTList( NTMap ) ) ) );
    }

    @RoutingConfigsTest
    void shouldProvideReaderAndRouterForSingleCoreSetup( Config config ) throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        CallableProcedure proc = newProcedure( coreTopologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( proc, config );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions( Config config ) throws Exception
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

        CallableProcedure proc = newProcedure( coreTopologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( proc, config );

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

    @RoutingConfigsTest
    void shouldReturnSelfIfOnlyMemberOfTheCluster( Config config ) throws Exception
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

        CallableProcedure proc = newProcedure( coreTopologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( proc, config );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnTheCoreLeaderForWriteAndReadReplicasAndCoresForReads( Config config ) throws Exception
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

        CallableProcedure procedure = newProcedure( topologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( procedure, config );

        // then
        AdvertisedSocketAddress coreBoltAddress = addressesForCore( 0, false ).connectors().boltAddress();
        AdvertisedSocketAddress readReplicaBoltAddress = addressesForReadReplica( 1 ).connectors().boltAddress();

        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( coreBoltAddress );
        if ( config.get( cluster_allow_reads_on_followers ) )
        {
            builder.readAddress( coreBoltAddress );
        }

        builder.readAddress( readReplicaBoltAddress );
        builder.routeAddress( coreBoltAddress );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnCoreMemberAsReadServerIfNoReadReplicasAvailable( Config config ) throws Exception
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

        CallableProcedure procedure = newProcedure( topologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( procedure, config );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnNoWriteEndpointsIfThereIsNoLeader( Config config ) throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        when( topologyService.localCoreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        CallableProcedure procedure = newProcedure( topologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( procedure, config );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader( Config config ) throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );

        when( topologyService.localCoreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        CallableProcedure procedure = newProcedure( topologyService, leaderLocator, config );

        // when
        ClusterView clusterView = run( procedure, config );

        // then

        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0, false ).connectors().boltAddress() );
        builder.routeAddress( addressesForCore( 0, false ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    void shouldReturnEndpointsInDifferentOrders() throws Exception
    {
        // given
        Config config = Config.defaults();
        CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0, false ) );
        coreMembers.put( member( 1 ), addressesForCore( 1, false ) );
        coreMembers.put( member( 2 ), addressesForCore( 2, false ) );

        CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        CallableProcedure proc = newProcedure( coreTopologyService, leaderLocator, config );

        // when
        Object[] endpoints = getEndpoints( proc );

        //then
        Object[] endpointsInDifferentOrder = getEndpoints( proc );
        for ( int i = 0; i < 100; i++ )
        {
            if ( Arrays.deepEquals( endpointsInDifferentOrder, endpoints ) )
            {
                endpointsInDifferentOrder = getEndpoints( proc );
            }
            else
            {
                //Different order of servers, no need to retry.
                break;
            }
        }
        assertThat( endpoints, not( equalTo( endpointsInDifferentOrder ) ) );
    }

    @Test
    void shouldHaveCorrectSignature()
    {
        // given
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        CallableProcedure proc = newProcedure( topologyService, leaderLocator, Config.defaults() );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.inputSignature(), containsInAnyOrder(
                FieldSignature.inputField( "context", NTMap ) ) );

        assertThat( signature.outputSignature(), containsInAnyOrder(
                FieldSignature.outputField( "ttl", NTInteger ),
                FieldSignature.outputField( "servers", NTList( NTMap ) ) ) );
    }

    @Test
    void shouldHaveCorrectNamespace()
    {
        // given
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        Config config = Config.defaults();

        CallableProcedure proc = newProcedure( topologyService, leaderLocator, config );

        // when
        QualifiedName name = proc.signature().name();

        // then
        assertEquals( new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ), name );
    }

    private Object[] getEndpoints( CallableProcedure proc ) throws ProcedureException
    {
        List<AnyValue[]> results = asList( proc.apply( null, new AnyValue[0], null ) );
        AnyValue[] rows = results.get( 0 );
        AnyValue[] servers = ((ListValue) rows[1]).asArray();
        MapValue readEndpoints = (MapValue) servers[1];
        MapValue routeEndpoints = (MapValue) servers[2];
        return new Object[]{readEndpoints.get( "addresses" ), routeEndpoints.get( "addresses" )};
    }

    private ClusterView run( CallableProcedure proc, Config config ) throws ProcedureException
    {
        final AnyValue[] rows = asList( proc.apply( null, new AnyValue[0], null ) ).get( 0 );
        assertEquals( longValue( config.get( routing_ttl ).getSeconds() ), /* ttl */rows[0] );
        return ClusterView.parse( (ListValue) rows[1] );
    }

    private CallableProcedure newProcedure( CoreTopologyService coreTopologyService, LeaderLocator leaderLocator, Config config )
    {
        return new GetServersProcedureForSingleDC( DEFAULT_NAMESPACE, coreTopologyService, leaderLocator, config, NullLogProvider.getInstance() );
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
