/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.SHARED;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.procedure.builtin.routing.Role.READ;
import static org.neo4j.procedure.builtin.routing.Role.ROUTE;
import static org.neo4j.procedure.builtin.routing.Role.WRITE;

@ClusterExtension
@TestInstance( Lifecycle.PER_METHOD )
class ClusterDiscoveryIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldFindReadWriteAndRouteServersWhenReadsOnFollowersAllowed() throws Exception
    {
        boolean allowReadsOnFollowers = true;

        Cluster cluster = startCluster( allowReadsOnFollowers );

        testReadWriteAndRouteServersDiscovery( cluster, allowReadsOnFollowers );
    }

    @Test
    void shouldFindReadWriteAndRouteServersWhenReadsOnFollowersDisallowed() throws Exception
    {
        boolean allowReadsOnFollowers = false;

        Cluster cluster = startCluster( allowReadsOnFollowers );

        testReadWriteAndRouteServersDiscovery( cluster, allowReadsOnFollowers );
    }

    private static void testReadWriteAndRouteServersDiscovery( Cluster cluster, boolean expectFollowersAsReadEndPoints ) throws Exception
    {
        for ( CoreClusterMember coreMember : cluster.coreMembers() )
        {
            verifyServersDiscovery( cluster, coreMember, expectFollowersAsReadEndPoints );
        }

        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            verifyServersDiscovery( readReplica );
        }
    }

    private static void verifyServersDiscovery( Cluster cluster, CoreClusterMember coreMember, boolean expectFollowersAsReadEndPoints ) throws Exception
    {
        List<Map<String,Object>> members = getMembers( coreMember.database() );

        Set<String> expectedWriteEndpoints = expectedWriteEndpoints( cluster );
        Set<String> expectedReadEndpoints = expectedReadEndpoints( cluster, expectFollowersAsReadEndPoints );
        Set<String> expectedRouteEndpoints = expectedRouteEndpoints( cluster );

        assertEquals( expectedWriteEndpoints, addresses( members, WRITE ) );
        assertEquals( expectedReadEndpoints, addresses( members, READ ) );
        assertEquals( expectedRouteEndpoints, addresses( members, ROUTE ) );
    }

    private static Set<String> expectedWriteEndpoints( Cluster cluster ) throws TimeoutException
    {
        CoreClusterMember leader = cluster.awaitLeader();
        return singleton( leader.boltAdvertisedAddress() );
    }

    private static Set<String> expectedReadEndpoints( Cluster cluster, boolean expectFollowersAsReadEndPoints ) throws TimeoutException
    {
        ClusterMember<?> leader = cluster.awaitLeader();
        Stream<ClusterMember<?>> cores = cluster.coreMembers().stream().map( identity() );
        Stream<ClusterMember<?>> readReplicas = cluster.readReplicas().stream().map( identity() );

        Stream<ClusterMember<?>> members = expectFollowersAsReadEndPoints ? Stream.concat( cores, readReplicas ) : readReplicas;

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

    private static void verifyServersDiscovery( ReadReplica readReplica ) throws Exception
    {
        List<Map<String,Object>> members = getMembers( readReplica.database() );

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
        try ( Result result = db.execute( "CALL dbms.routing.getRoutingTable({})" ) )
        {
            Map<String,Object> record = Iterators.single( result );
            return (List<Map<String,Object>>) record.get( "servers" );
        }
    }

    private Cluster startCluster( boolean allowReadsOnFollowers ) throws Exception
    {
        Cluster cluster = clusterFactory.createCluster( newClusterConfig( allowReadsOnFollowers ) );
        cluster.start();
        return cluster;
    }

    private static ClusterConfig newClusterConfig( boolean allowReadsOnFollowers )
    {
        return clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withDiscoveryServiceType( SHARED )
                .withSharedCoreParam( cluster_allow_reads_on_followers, Boolean.toString( allowReadsOnFollowers ) )
                .withSharedReadReplicaParam( cluster_allow_reads_on_followers, Boolean.toString( allowReadsOnFollowers ) );
    }
}
