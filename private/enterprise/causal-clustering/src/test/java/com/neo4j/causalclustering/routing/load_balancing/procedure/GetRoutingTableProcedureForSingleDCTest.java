/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.routing.load_balancing.DefaultLeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static com.neo4j.causalclustering.discovery.TestTopology.readReplicaInfoMap;
import static com.neo4j.causalclustering.identity.RaftTestMember.server;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_leader;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

// TODO: Better tests for LeaderLocator function.
class GetRoutingTableProcedureForSingleDCTest
{
    private DatabaseManager<?> databaseManager;
    private NamedDatabaseId namedDatabaseId;
    private RaftGroupId raftGroupId;
    private DatabaseAvailabilityGuard availabilityGuard;
    private MutableLeaderService leaderService;
    private CoreTopologyService coreTopologyService;
    // given

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
                Arguments.of( Config.defaults( Map.of( cluster_allow_reads_on_followers, true, cluster_allow_reads_on_leader, false ) ) ),
                Arguments.of( Config.defaults( Map.of( cluster_allow_reads_on_followers, false, cluster_allow_reads_on_leader, false ) ) ),
                Arguments.of( Config.defaults( Map.of( cluster_allow_reads_on_followers, true, cluster_allow_reads_on_leader, true ) ) ),
                Arguments.of( Config.defaults( Map.of( cluster_allow_reads_on_followers, false, cluster_allow_reads_on_leader, true ) ) )
        );
    }

    @BeforeEach
    private void setUp()
    {
        var databaseManager = new StubClusteredDatabaseManager();
        this.namedDatabaseId = databaseManager.databaseIdRepository().getByName( "my_test_database" ).get();
        this.raftGroupId = RaftGroupId.from( namedDatabaseId.databaseId() );
        this.availabilityGuard = mock( DatabaseAvailabilityGuard.class );
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( namedDatabaseId ).withDatabaseAvailabilityGuard( availabilityGuard ).register();
        this.databaseManager = databaseManager;
        coreTopologyService = mock( CoreTopologyService.class );
        var discoveryDatabaseState = mock( DiscoveryDatabaseState.class );
        when( discoveryDatabaseState.operatorState() ).thenReturn( EnterpriseOperatorState.STARTED );
        when( coreTopologyService.lookupDatabaseState( any(), any() ) ).thenReturn( discoveryDatabaseState );

        leaderService = new MutableLeaderService( namedDatabaseId, coreTopologyService );
    }

    @RoutingConfigsTest
    void ttlShouldBeInSeconds( Config config ) throws Exception
    {
        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ) );
        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var leaderService = newLeaderService( coreTopologyService );

        // set the TTL in minutes
        config.set( routing_ttl, Duration.ofMinutes( 10 ) );

        var proc = newProcedure( coreTopologyService, leaderService, config );

        // when
        var results = asList( proc.apply( null, inputParameters(), null ) );

        // then
        var rows = results.get( 0 );
        var ttlInSeconds = (LongValue) rows[0];
        assertEquals( longValue( 600 ), ttlInSeconds );
    }

    @RoutingConfigsTest
    void shouldHaveCorrectSignature( Config config )
    {
        // given
        var proc = newProcedure( null, null, config );

        // when
        var signature = proc.signature();

        // then
        assertEquals( List.of( inputField( "context", NTMap ), inputField( "database", NTString, nullValue( NTString ) ) ), signature.inputSignature() );
        assertEquals( List.of( outputField( "ttl", NTInteger ), outputField( "servers", NTList( NTMap ) ) ), signature.outputSignature() );
        assertTrue( signature.systemProcedure() );
    }

    @RoutingConfigsTest
    void shouldProvideReaderAndRouterForSingleCoreSetup( Config config ) throws Exception
    {
        // given
        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var proc = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( proc, config );

        // then
        var builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions( Config config ) throws Exception
    {
        // given
        leaderService.setLeader( server( 0 ) );

        var coreMembers = Map.of(
                server( 0 ), addressesForCore( 0 ),
                server( 1 ), addressesForCore( 1 ),
                server( 2 ), addressesForCore( 2 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var proc = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( proc, config );

        // then
        var builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.readAddress( addressesForCore( 1 ).connectors().clientBoltAddress() );
        builder.readAddress( addressesForCore( 2 ).connectors().clientBoltAddress() );
        if ( config.get( cluster_allow_reads_on_leader ) )
        {
            builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        }
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 1 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 2 ).connectors().clientBoltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnSelfIfOnlyMemberOfTheCluster( Config config ) throws Exception
    {
        // given
        leaderService.setLeader( server( 0 ) );
        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var proc = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( proc, config );

        // then
        var builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnTheCoreLeaderForWriteAndReadReplicasAndCoresForReads( Config config ) throws Exception
    {
        // given
        leaderService.setLeader( server( 0 ) );
        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ),
                                  server( 1 ), addressesForCore( 1 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, readReplicaInfoMap( 1 ) );

        var procedure = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( procedure, config );

        // then
        var coreBoltAddress0 = addressesForCore( 0 ).connectors().clientBoltAddress();
        var coreBoltAddress1 = addressesForCore( 1 ).connectors().clientBoltAddress();
        var readReplicaBoltAddress = addressesForReadReplica( 1 ).connectors().clientBoltAddress();

        var builder = new ClusterView.Builder();
        builder.writeAddress( coreBoltAddress0 );
        if ( config.get( cluster_allow_reads_on_followers ) )
        {
            builder.readAddress( coreBoltAddress1 );
        }
        if ( config.get( cluster_allow_reads_on_leader ) )
        {
            builder.readAddress( coreBoltAddress0 );
        }

        builder.readAddress( readReplicaBoltAddress );
        builder.routeAddress( coreBoltAddress0 );
        builder.routeAddress( coreBoltAddress1 );
        System.out.println( clusterView );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnCoreMemberAsReadServerIfNoReadReplicasAvailable( Config config ) throws Exception
    {
        // given
        leaderService.setLeader( server( 0 ) );

        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var procedure = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( procedure, config );

        // then
        var builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnNoWriteEndpointsIfThereIsNoLeader( Config config ) throws Exception
    {
        // given
        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var procedure = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( procedure, config );

        // then
        var builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @RoutingConfigsTest
    void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader( Config config ) throws Exception
    {
        // given
        leaderService.setLeader( server( 1 ) );
        var coreMembers = Map.of( server( 0 ), addressesForCore( 0 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var procedure = newProcedure( coreTopologyService, leaderService, config );

        // when
        var clusterView = run( procedure, config );

        // then

        var builder = new ClusterView.Builder();
        builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    void shouldReturnEndpointsInDifferentOrders() throws Exception
    {
        // given
        var config = Config.defaults();
        leaderService.setLeader( server( 0 ) );

        var coreMembers = Map.of(
                server( 0 ), addressesForCore( 0 ),
                server( 1 ), addressesForCore( 1 ),
                server( 2 ), addressesForCore( 2 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap() );

        var proc = newProcedure( coreTopologyService, leaderService, config );

        // when
        var endpoints = getEndpoints( proc );

        //then
        var endpointsInDifferentOrder = getEndpoints( proc );
        for ( var i = 0; i < 100; i++ )
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
    void shouldHaveCorrectSignature() throws Exception
    {
        // given
        leaderService.setLeader( server( 1 ) );

        var proc = newProcedure( coreTopologyService, leaderService, Config.defaults() );

        // when
        var signature = proc.signature();

        // then
        assertEquals( List.of( inputField( "context", NTMap ), inputField( "database", NTString, nullValue( NTString ) ) ), signature.inputSignature() );

        assertEquals( List.of( outputField( "ttl", NTInteger ), outputField( "servers", NTList( NTMap ) ) ), signature.outputSignature() );
    }

    @Test
    void shouldHaveCorrectNamespace()
    {
        // given
        var config = Config.defaults();

        var proc = newProcedure( coreTopologyService, leaderService, config );

        // when
        var name = proc.signature().name();

        // then
        assertEquals( new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ), name );
    }

    @Test
    void shouldThrowWhenDatabaseDoesNotExist() throws Exception
    {
        TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
        var databaseManager = new StubClusteredDatabaseManager( databaseIdRepository );
        var databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        databaseIdRepository.filter( databaseId.name() );
        leaderService.setLeader( server( 0 ) );

        var proc = newProcedure( coreTopologyService, leaderService, databaseManager );

        var error = assertThrows( ProcedureException.class, () -> run( proc, databaseId, Config.defaults() ) );
        assertEquals( Status.Database.DatabaseNotFound, error.status() );
    }

    @RoutingConfigsTest
    void shouldNotThrowWhenDatabaseIsStopped( Config config ) throws Exception
    {
        var databaseManager = new StubClusteredDatabaseManager();
        var databaseId = databaseManager.databaseIdRepository().getByName( "stopped database" ).get();
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withStoppedDatabase().register();
        leaderService.setLeader( databaseId, server( 0 ) );

        var coreMembers = Map.of(
                server( 0 ), addressesForCore( 0 ),
                server( 1 ), addressesForCore( 1 ),
                server( 2 ), addressesForCore( 2 ) );

        setupCoreTopologyService( coreTopologyService, coreMembers, emptyMap(), databaseId, raftGroupId );

        var proc = newProcedure( coreTopologyService, leaderService, databaseManager, config );

        var clusterView = run( proc, databaseId, config );

        var builder = new ClusterView.Builder();
        builder.writeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        if ( config.get( cluster_allow_reads_on_leader ) )
        {
            builder.readAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        }
        builder.readAddress( addressesForCore( 1 ).connectors().clientBoltAddress() );
        builder.readAddress( addressesForCore( 2 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 0 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 1 ).connectors().clientBoltAddress() );
        builder.routeAddress( addressesForCore( 2 ).connectors().clientBoltAddress() );
        assertEquals( builder.build(), clusterView );
    }

    @Test
    void shouldThrowWhenTopologyServiceContainsNoInfoAboutTheDatabaseButDatabaseExists() throws Exception
    {
        var unknownNamedDatabaseId = databaseManager.databaseIdRepository().getByName( "unknown" ).get();
        when( coreTopologyService.coreTopologyForDatabase( unknownNamedDatabaseId ) )
                .thenReturn( DatabaseCoreTopology.empty( unknownNamedDatabaseId.databaseId() ) );
        when( coreTopologyService.readReplicaTopologyForDatabase( unknownNamedDatabaseId ) )
                .thenReturn( DatabaseReadReplicaTopology.empty( unknownNamedDatabaseId.databaseId() ) );

        leaderService.setLeader( server( 0 ) );
        var config = Config.defaults();

        var proc = newProcedure( coreTopologyService, leaderService, config );

        var error = assertThrows( ProcedureException.class, () -> run( proc, unknownNamedDatabaseId, config ) );
        assertEquals( Status.Database.DatabaseUnavailable, error.status() );
    }

    private Object[] getEndpoints( CallableProcedure proc ) throws ProcedureException
    {
        var results = asList( proc.apply( null, inputParameters(), null ) );
        var rows = results.get( 0 );
        var servers = ((ListValue) rows[1]).asArray();
        var readEndpoints = (MapValue) servers[1];
        var routeEndpoints = (MapValue) servers[2];
        return new Object[]{readEndpoints.get( "addresses" ), routeEndpoints.get( "addresses" )};
    }

    private ClusterView run( CallableProcedure proc, Config config ) throws ProcedureException
    {
        return run( proc, namedDatabaseId, config );
    }

    private ClusterView run( CallableProcedure proc, NamedDatabaseId namedDatabaseId, Config config ) throws ProcedureException
    {
        var rows = asList( proc.apply( null, inputParameters( namedDatabaseId ), null ) ).get( 0 );
        assertEquals( longValue( config.get( routing_ttl ).getSeconds() ), /* ttl */rows[0] );
        return ClusterView.parse( (ListValue) rows[1] );
    }

    private CallableProcedure newProcedure( CoreTopologyService coreTopologyService, LeaderService leaderService, DatabaseManager<?> databaseManager )
    {
        return newProcedure( coreTopologyService, leaderService, databaseManager, Config.defaults() );
    }

    private CallableProcedure newProcedure( CoreTopologyService coreTopologyService, LeaderService leaderService, Config config )
    {
        return newProcedure( coreTopologyService, leaderService, databaseManager, config );
    }

    private CallableProcedure newProcedure( CoreTopologyService coreTopologyService, LeaderService leaderService, DatabaseManager<?> databaseManager,
            Config config )
    {
        return new GetRoutingTableProcedureForSingleDC(
                DEFAULT_NAMESPACE,
                coreTopologyService,
                leaderService,
                databaseManager,
                config,
                NullLogProvider.getInstance() );
    }

    private AnyValue[] inputParameters()
    {
        return inputParameters( namedDatabaseId );
    }

    private static AnyValue[] inputParameters( NamedDatabaseId namedDatabaseId )
    {
        return new AnyValue[]{MapValue.EMPTY, stringValue( namedDatabaseId.name() )};
    }

    private void setupCoreTopologyService( CoreTopologyService topologyService, Map<ServerId,CoreServerInfo> cores, Map<ServerId,ReadReplicaInfo> readReplicas )
    {
        setupCoreTopologyService( topologyService, cores, readReplicas, namedDatabaseId, raftGroupId );
    }

    private static void setupCoreTopologyService( CoreTopologyService topologyService, Map<ServerId,CoreServerInfo> cores,
                                                  Map<ServerId,ReadReplicaInfo> readReplicas, NamedDatabaseId namedDatabaseId, RaftGroupId raftGroupId )
    {
        when( topologyService.allCoreServers() ).thenReturn( cores );
        when( topologyService.allReadReplicas() ).thenReturn( readReplicas );
        when( topologyService.coreTopologyForDatabase( namedDatabaseId ) )
                .thenReturn( new DatabaseCoreTopology( namedDatabaseId.databaseId(), raftGroupId, cores ) );
        when( topologyService.readReplicaTopologyForDatabase( namedDatabaseId ) )
                .thenReturn( new DatabaseReadReplicaTopology( namedDatabaseId.databaseId(), readReplicas ) );
    }

    private LeaderService newLeaderService( CoreTopologyService coreTopologyService )
    {
        return new DefaultLeaderService( coreTopologyService, nullLogProvider() );
    }

    private static class ClusterView
    {
        private final Map<Role,Set<SocketAddress>> clusterView;

        private ClusterView( Map<Role,Set<SocketAddress>> clusterView )
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
            var that = (ClusterView) o;
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
            var view = new HashMap<Role,Set<SocketAddress>>();
            for ( var value : result )
            {
                var single = (MapValue) value;
                var role = Role.valueOf( ((TextValue) single.get( "role" )).stringValue() );
                var addresses = parseAdresses( (ListValue) single.get( "addresses" ) );
                assertFalse( view.containsKey( role ) );
                view.put( role, addresses );
            }

            return new ClusterView( view );
        }

        private static Set<SocketAddress> parseAdresses( ListValue addresses )
        {

            var list = new ArrayList<SocketAddress>( addresses.size() );
            for ( var address : addresses )
            {
                list.add( parse( ((TextValue) address).stringValue() ) );
            }
            var set = new HashSet<>( list );
            assertEquals( list.size(), set.size() );
            return set;
        }

        private static SocketAddress parse( String address )
        {
            var split = address.split( ":" );
            assertEquals( 2, split.length );
            return new SocketAddress( split[0], Integer.parseInt( split[1] ) );
        }

        static class Builder
        {
            private final Map<Role,Set<SocketAddress>> view = new HashMap<>();

            void readAddress( SocketAddress address )
            {
                addAddress( Role.READ, address );
            }

            void writeAddress( SocketAddress address )
            {
                addAddress( Role.WRITE, address );
            }

            void routeAddress( SocketAddress address )
            {
                addAddress( Role.ROUTE, address );
            }

            private void addAddress( Role role, SocketAddress address )
            {
                var advertisedSocketAddresses = view.computeIfAbsent( role, k -> new HashSet<>() );
                advertisedSocketAddresses.add( address );
            }

            public ClusterView build()
            {
                return new ClusterView( view );
            }
        }
    }

    private static class MutableLeaderService implements LeaderService
    {
        Map<NamedDatabaseId,ServerId> leaders = new HashMap<>();
        private NamedDatabaseId defaultDb;
        private CoreTopologyService coreTopologyService;

        MutableLeaderService( NamedDatabaseId defaultDb, CoreTopologyService coreTopologyService )
        {
            this.defaultDb = defaultDb;
            this.coreTopologyService = coreTopologyService;
        }

        public Optional<ServerId> getLeaderId( NamedDatabaseId namedDatabaseId )
        {
            return Optional.ofNullable( leaders.get( namedDatabaseId ) );
        }

        @Override
        public Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId )
        {
            var leader = leaders.get( namedDatabaseId );
            if ( leader != null )
            {
                return Optional.ofNullable( coreTopologyService.allCoreServers().get( leader ) )
                        .map( coreInfo -> coreInfo.connectors().clientBoltAddress() );
            }
            return Optional.empty();
        }

        void setLeader( ServerId leader )
        {
            setLeader( defaultDb, leader );
        }

        void setLeader( NamedDatabaseId namedDatabaseId, ServerId leader )
        {
            this.leaders.put( namedDatabaseId, leader );
        }
    }
}
