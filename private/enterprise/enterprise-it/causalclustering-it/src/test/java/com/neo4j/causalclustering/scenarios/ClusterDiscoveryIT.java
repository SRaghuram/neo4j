/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
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

        var config = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParam( BoltConnector.connector_routing_enabled, "true" )
                .withSharedReadReplicaParam( BoltConnector.connector_routing_enabled, "true" );

        var cluster = clusterFactory.createCluster( config );
        cluster.start();

        var expected = cluster.allMembers().stream()
                              .map( ClusterMember::intraClusterBoltAdvertisedAddress )
                              .collect( Collectors.toSet() );

        var topologyService = cluster.awaitLeader().resolveDependency( SYSTEM_DATABASE_NAME, TopologyService.class );
        assertEventually( () -> intraClusterAddresses( topologyService ), equalityCondition( expected ), 30, SECONDS );
    }

    private Set<String> intraClusterAddresses( TopologyService topologyService )
    {
        var serverInfos = Stream.concat( topologyService.allCoreServers().values().stream(), topologyService.allReadReplicas().values().stream() );
        return serverInfos.flatMap( info -> info.connectors().intraClusterBoltAddress().stream() )
                          .map( SocketAddress::toString )
                          .collect( Collectors.toSet() );
    }

    @Test
    void shouldFindReadWriteAndRouteServersWhenReadsOnFollowersAllowed() throws Exception
    {
        var allowReadsOnFollowers = true;

        var cluster = startCluster( allowReadsOnFollowers );

        testReadWriteAndRouteServersDiscovery( cluster, allowReadsOnFollowers );
    }

    @Test
    void shouldFindReadWriteAndRouteServersWhenReadsOnFollowersDisallowed() throws Exception
    {
        var allowReadsOnFollowers = false;

        var cluster = startCluster( allowReadsOnFollowers );

        testReadWriteAndRouteServersDiscovery( cluster, allowReadsOnFollowers );
    }

    private static void testReadWriteAndRouteServersDiscovery( Cluster cluster, boolean expectFollowersAsReadEndPoints ) throws Exception
    {
        for ( var coreMember : cluster.coreMembers() )
        {
            verifyServersDiscovery( cluster, coreMember, expectFollowersAsReadEndPoints );
        }

        for ( var readReplica : cluster.readReplicas() )
        {
            verifyServersDiscovery( readReplica );
        }
    }

    private static void verifyServersDiscovery( Cluster cluster, CoreClusterMember coreMember, boolean expectFollowersAsReadEndPoints ) throws Exception
    {
        var expectedWriteEndpoints = expectedWriteEndpoints( cluster );
        var expectedReadEndpoints = expectedReadEndpoints( cluster, expectFollowersAsReadEndPoints );
        var expectedRouteEndpoints = expectedRouteEndpoints( cluster );
        var routingTableMatcher = new RoutingTableMatcher( expectedWriteEndpoints, expectedReadEndpoints, expectedRouteEndpoints );

        assertEventually( () -> getMembers( coreMember.defaultDatabase() ), new HamcrestCondition<>( routingTableMatcher ), 30, SECONDS );
    }

    private static Set<String> expectedWriteEndpoints( Cluster cluster ) throws TimeoutException
    {
        var leader = cluster.awaitLeader();
        return singleton( leader.boltAdvertisedAddress() );
    }

    private static Set<String> expectedReadEndpoints( Cluster cluster, boolean expectFollowersAsReadEndPoints ) throws TimeoutException
    {
        var leader = cluster.awaitLeader();
        var cores = cluster.coreMembers().stream();
        var readReplicas = cluster.readReplicas().stream();

        var members = expectFollowersAsReadEndPoints ? Stream.concat( cores, readReplicas ) : readReplicas;

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

    private static void verifyServersDiscovery( ReadReplica readReplica )
    {
        var members = getMembers( readReplica.defaultDatabase() );

        assertEquals( singleton( readReplica.boltAdvertisedAddress() ), addresses( members, READ ) );
        assertEquals( singleton( readReplica.boltAdvertisedAddress() ), addresses( members, ROUTE ) );
        assertEquals( emptySet(), addresses( members, WRITE ) );
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

    private Cluster startCluster( boolean allowReadsOnFollowers ) throws Exception
    {
        var cluster = clusterFactory.createCluster( newClusterConfig( allowReadsOnFollowers ) );
        cluster.start();
        return cluster;
    }

    private static ClusterConfig newClusterConfig( boolean allowReadsOnFollowers )
    {
        return clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParam( cluster_allow_reads_on_followers, Boolean.toString( allowReadsOnFollowers ) )
                .withSharedReadReplicaParam( cluster_allow_reads_on_followers, Boolean.toString( allowReadsOnFollowers ) );
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
}
