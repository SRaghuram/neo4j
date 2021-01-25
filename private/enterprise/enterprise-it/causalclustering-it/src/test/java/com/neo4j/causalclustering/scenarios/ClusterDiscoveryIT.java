/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.procedure.builtin.routing.Role.READ;
import static org.neo4j.procedure.builtin.routing.Role.ROUTE;
import static org.neo4j.procedure.builtin.routing.Role.WRITE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@TestInstance( Lifecycle.PER_METHOD )
class ClusterDiscoveryIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldFindIntraClusterBoltAddress() throws Exception
    {
        var cluster = startCluster( ReadsOnFollowers.DENY, ServerSideRouting.ENABLED );

        var expected = cluster.allMembers().stream()
                              .map( ClusterMember::intraClusterBoltAdvertisedAddress )
                              .collect( Collectors.toSet() );

        var topologyService = cluster.awaitLeader().resolveDependency( SYSTEM_DATABASE_NAME, TopologyService.class );
        assertEventually( () -> intraClusterAddresses( topologyService ), equalityCondition( expected ), 30, SECONDS );

        cluster.shutdown();
    }

    @ParameterizedTest
    @EnumSource()
    void shouldFindReadWriteAndRouteServersWhenReadsOnFollowersAllowed( ServerSideRouting serverSideRouting ) throws Exception
    {
        var readsOnFollowers = ReadsOnFollowers.ALLOW;

        var cluster = startCluster( readsOnFollowers, serverSideRouting );

        testReadWriteAndRouteServersDiscovery( cluster, readsOnFollowers, serverSideRouting );

        cluster.shutdown();
    }

    @ParameterizedTest
    @EnumSource()
    void shouldFindReadWriteAndRouteServersWhenReadsOnFollowersDisallowed( ServerSideRouting serverSideRouting ) throws Exception
    {
        var readsOnFollowers = ReadsOnFollowers.DENY;

        var cluster = startCluster( readsOnFollowers, serverSideRouting );

        testReadWriteAndRouteServersDiscovery( cluster, readsOnFollowers, serverSideRouting );

        cluster.shutdown();
    }

    private Set<String> intraClusterAddresses( TopologyService topologyService )
    {
        var serverInfos = Stream.concat(
                topologyService.allCoreServers().values().stream(),
                topologyService.allReadReplicas().values().stream()
        );
        return serverInfos.flatMap( info -> info.connectors().intraClusterBoltAddress().stream() )
                          .map( SocketAddress::toString )
                          .collect( Collectors.toSet() );
    }

    private static void testReadWriteAndRouteServersDiscovery( Cluster cluster, ReadsOnFollowers readsOnFollowers, ServerSideRouting serverSideRouting )
            throws Exception
    {
        for ( var coreMember : cluster.coreMembers() )
        {
            verifyServersDiscovery( cluster, coreMember, readsOnFollowers );
        }

        for ( var readReplica : cluster.readReplicas() )
        {
            verifyServersDiscovery( readReplica, serverSideRouting );
        }
    }

    private static void verifyServersDiscovery( Cluster cluster, CoreClusterMember coreMember, ReadsOnFollowers readsOnFollowers ) throws Exception
    {
        var expectedWriteEndpoints = expectedWriteEndpoints( cluster );
        var expectedReadEndpoints = expectedReadEndpoints( cluster, readsOnFollowers );
        var expectedRouteEndpoints = expectedRouteEndpoints( cluster );
        var routingTableMatcher = new RoutingTableMatcher( expectedWriteEndpoints, expectedReadEndpoints, expectedRouteEndpoints );

        assertEventually( () -> getMembers( coreMember.defaultDatabase() ), new HamcrestCondition<>( routingTableMatcher ), 30, SECONDS );
    }

    private static Set<String> expectedWriteEndpoints( Cluster cluster ) throws TimeoutException
    {
        var leader = cluster.awaitLeader();
        return singleton( leader.boltAdvertisedAddress() );
    }

    private static Set<String> expectedReadEndpoints( Cluster cluster, ReadsOnFollowers readsOnFollowers ) throws TimeoutException
    {
        var leader = cluster.awaitLeader();
        var cores = cluster.coreMembers().stream();
        var readReplicas = cluster.readReplicas().stream();

        var members = readsOnFollowers.equals( ReadsOnFollowers.ALLOW ) ? Stream.concat( cores, readReplicas ) : readReplicas;

        return members.filter( member -> !leader.equals( member ) )
                .map( ClusterMember::boltAdvertisedAddress )
                .collect( toSet() );
    }

    private static Set<String> expectedRouteEndpoints( Cluster cluster )
    {
        return cluster.coreMembers()
                .stream()
                .map( CoreClusterMember::boltAdvertisedAddress )
                .collect( toSet() );
    }

    private static void verifyServersDiscovery( ReadReplica readReplica, ServerSideRouting serverSideRouting )
    {
        var members = getMembers( readReplica.defaultDatabase() );

        Set<String> selfAddressOnly = singleton( readReplica.boltAdvertisedAddress() );
        assertEquals( selfAddressOnly, addresses( members, READ ) );
        assertEquals( selfAddressOnly, addresses( members, ROUTE ) );
        if ( serverSideRouting.equals( ServerSideRouting.ENABLED ) )
        {
            assertEquals( selfAddressOnly, addresses( members, WRITE ) );
        }
        else
        {
            assertEquals( emptySet(), addresses( members, WRITE ) );
        }

    }

    @SuppressWarnings( "unchecked" )
    private static Set<String> addresses( List<Map<String,Object>> procedureResult, Role role )
    {
        return procedureResult.stream()
                .filter( x -> role.toString().equals( x.get( "role" ) ) )
                .flatMap( x -> ((List<String>) x.get( "addresses" )).stream() )
                .collect( toSet() );
    }

    @SuppressWarnings( "unchecked" )
    private static List<Map<String,Object>> getMembers( GraphDatabaseAPI db )
    {
        try ( var transaction = db.beginTx() )
        {
            try ( var result = transaction.execute( "CALL dbms.routing.getRoutingTable({})" ) )
            {
                var record = Iterators.single( result );
                return (List<Map<String,Object>>) record.get( "servers" );
            }
        }
    }

    private Cluster startCluster( ReadsOnFollowers readsOnFollowers, ServerSideRouting serverSideRouting ) throws Exception
    {
        var cluster = clusterFactory.createCluster( newClusterConfig( readsOnFollowers, serverSideRouting ) );
        cluster.start();
        return cluster;
    }

    private ClusterConfig newClusterConfig( ReadsOnFollowers readsOnFollowers, ServerSideRouting serverSideRouting )
    {
        return clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParams( readsOnFollowers.asConfig() )
                .withSharedReadReplicaParams( readsOnFollowers.asConfig() )
                .withSharedCoreParams( serverSideRouting.asConfig() )
                .withSharedReadReplicaParams( serverSideRouting.asConfig() );
    }

    private static class RoutingTableMatcher extends TypeSafeMatcher<List<Map<String,Object>>>
    {
        final Set<String> expectedWriteEndpoints;
        final Set<String> expectedReadEndpoints;
        final Set<String> expectedRouteEndpoints;

        RoutingTableMatcher( Set<String> expectedWriteEndpoints, Set<String> expectedReadEndpoints, Set<String> expectedRouteEndpoints )
        {
            this.expectedWriteEndpoints = expectedWriteEndpoints;
            this.expectedReadEndpoints = expectedReadEndpoints;
            this.expectedRouteEndpoints = expectedRouteEndpoints;
        }

        @Override
        protected boolean matchesSafely( List<Map<String,Object>> procedureResponse )
        {
            return Objects.equals( expectedWriteEndpoints, addresses( procedureResponse, WRITE ) ) &&
                   Objects.equals( expectedReadEndpoints, addresses( procedureResponse, READ ) ) &&
                   Objects.equals( expectedRouteEndpoints, addresses( procedureResponse, ROUTE ) );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "write endpoints: " ).appendValue( expectedWriteEndpoints )
                    .appendText( "read endpoints: " ).appendValue( expectedReadEndpoints )
                    .appendText( "route endpoints: " ).appendValue( expectedRouteEndpoints );
        }
    }

    private enum ReadsOnFollowers
    {
        ALLOW,
        DENY;

        Map<String,String> asConfig()
        {
            return Map.of( cluster_allow_reads_on_followers.name(), Boolean.toString( this.equals( ALLOW ) ) );
        }
    }

    private enum ServerSideRouting
    {
        ENABLED,
        DISABLED;

        Map<String,String> asConfig()
        {
            return Map.of( GraphDatabaseSettings.routing_enabled.name(), Boolean.toString( this.equals( ENABLED ) ) );
        }
    }
}
