/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.server_groups;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class ClusterOverviewProcedureIT
{
    private static final int CORES = 3;
    private static final int REPLICAS = 2;
    private static final int EXPECTED_COLUMNS = 4;

    private static final TestDatabaseIdRepository DATABASE_ID_REPOSITORY = new TestDatabaseIdRepository();
    private static final DatabaseId DEFAULT_DB = DATABASE_ID_REPOSITORY.defaultDatabase();

    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    @BeforeAll
    static void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( buildClusterConfig() );
        cluster.start();
    }

    @Test
    void shouldReturnClusterOverview() throws Exception
    {
        var leader = cluster.awaitLeader();
        var membersById = cluster.allMembers().stream().collect( toMap( ClusterMember::id, identity() ) );
        assertEquals( CORES + REPLICAS, membersById.size() );

        var clusterOverviewMatcher = buildClusterOverviewMatcher( membersById );

        for ( var member : membersById.values() )
        {
            assertEventually( () -> invokeClusterOverviewProcedure( member ), clusterOverviewMatcher, 2, MINUTES );
        }
    }

    private static List<Map<String,Object>> invokeClusterOverviewProcedure( ClusterMember member )
    {
        var db = member.defaultDatabase();
        try ( var transaction = db.beginTx() )
        {
            try ( var result = transaction.execute( "CALL dbms.cluster.overview()" ) )
            {
                return asList( result );
            }
        }
    }

    private static ClusterOverviewMatcher buildClusterOverviewMatcher( Map<MemberId,ClusterMember> membersById )
    {
        var sortedMemberIds = membersById.keySet()
                .stream()
                .sorted( Comparator.comparing( MemberId::getUuid ) )
                .collect( toList() );

        var expectedMemberIds = sortedMemberIds.stream()
                .map( id -> id.getUuid().toString() )
                .collect( toList() );

        var expectedAddresses = sortedMemberIds.stream()
                .map( membersById::get )
                .map( ClusterOverviewProcedureIT::expectedAddresses )
                .collect( toList() );

        var expectedDatabases = sortedMemberIds.stream()
                .map( membersById::get )
                .map( ClusterOverviewProcedureIT::expectedDatabases )
                .collect( toList() );

        var expectedGroups = sortedMemberIds.stream()
                .map( membersById::get )
                .map( ClusterOverviewProcedureIT::expectedGroups )
                .collect( toList() );

        return new ClusterOverviewMatcher( expectedMemberIds, expectedAddresses, expectedDatabases, expectedGroups );
    }

    private static List<String> expectedAddresses( ClusterMember member )
    {
        return member.clientConnectorAddresses()
                .uriList()
                .stream()
                .map( URI::toString )
                .collect( toList() );
    }

    private static Map<String,String> expectedDatabases( ClusterMember member )
    {
        return Map.of(
                SYSTEM_DATABASE_ID.name(), expectedRole( member, SYSTEM_DATABASE_ID ).toString(),
                DEFAULT_DB.name(), expectedRole( member, DEFAULT_DB ).toString() );
    }

    private static RoleInfo expectedRole( ClusterMember member, DatabaseId databaseId )
    {
        if ( isLeader( member, databaseId ) )
        {
            return RoleInfo.LEADER;
        }
        if ( isCore( member ) )
        {
            return RoleInfo.FOLLOWER;
        }
        if ( isReadReplica( member ) )
        {
            return RoleInfo.READ_REPLICA;
        }
        throw new IllegalArgumentException( "Unable to find role for " + member );
    }

    private static List<String> expectedGroups( ClusterMember member )
    {
        if ( isCore( member ) )
        {
            return List.of( "core", "eu-" + member.serverId() );
        }
        if ( isReadReplica( member ) )
        {
            return List.of( "replica", "us-" + member.serverId() );
        }
        throw new IllegalArgumentException( "Unable to find groups for " + member );
    }

    private static boolean isLeader( ClusterMember member, DatabaseId databaseId )
    {
        CoreClusterMember leader = cluster.getMemberWithAnyRole( databaseId.name(), Role.LEADER );
        return Objects.equals( member, leader );
    }

    private static boolean isCore( ClusterMember member )
    {
        return cluster.coreMembers().contains( member );
    }

    private static boolean isReadReplica( ClusterMember member )
    {
        return cluster.readReplicas().contains( member );
    }

    private static ClusterConfig buildClusterConfig()
    {
        return clusterConfig()
                .withNumberOfCoreMembers( CORES )
                .withNumberOfReadReplicas( REPLICAS )
                .withInstanceCoreParam( server_groups, id -> "core,eu-" + id )
                .withInstanceReadReplicaParam( server_groups, id -> "replica,us-" + id );
    }

    private static class ClusterOverviewMatcher extends TypeSafeMatcher<List<Map<String,Object>>>
    {
        final List<String> expectedMemberIds;
        final List<List<String>> expectedAddresses;
        final List<Map<String,String>> expectedDatabases;
        final List<List<String>> expectedGroups;

        ClusterOverviewMatcher( List<String> expectedMemberIds, List<List<String>> expectedAddresses,
                List<Map<String,String>> expectedDatabases, List<List<String>> expectedGroups )
        {
            this.expectedMemberIds = expectedMemberIds;
            this.expectedAddresses = expectedAddresses;
            this.expectedDatabases = expectedDatabases;
            this.expectedGroups = expectedGroups;
        }

        @Override
        protected boolean matchesSafely( List<Map<String,Object>> rows )
        {
            if ( rows.size() != CORES + REPLICAS )
            {
                return false;
            }

            return numberOfColumnsMatch( rows ) &&
                   memberIdsMatch( rows ) &&
                   addressesMatch( rows ) &&
                   databasesMatch( rows ) &&
                   groupsMatch( rows );
        }

        boolean memberIdsMatch( List<Map<String,Object>> rows )
        {
            var actualMemberIds = rows.stream()
                    .map( row -> (String) row.get( "id" ) )
                    .collect( toList() );

            return Objects.equals( expectedMemberIds, actualMemberIds );
        }

        @SuppressWarnings( "unchecked" )
        boolean addressesMatch( List<Map<String,Object>> rows )
        {
            var actualAddresses = rows.stream()
                    .map( row -> (List<String>) row.get( "addresses" ) )
                    .collect( toList() );

            return Objects.equals( expectedAddresses, actualAddresses );
        }

        @SuppressWarnings( "unchecked" )
        boolean databasesMatch( List<Map<String,Object>> rows )
        {
            var actualDatabases = rows.stream()
                    .map( row -> (Map<String,String>) row.get( "databases" ) )
                    .collect( toList() );

            return Objects.equals( expectedDatabases, actualDatabases );
        }

        @SuppressWarnings( "unchecked" )
        boolean groupsMatch( List<Map<String,Object>> rows )
        {
            var actualGroups = rows.stream()
                    .map( row -> (List<String>) row.get( "groups" ) )
                    .collect( toList() );

            return Objects.equals( expectedGroups, actualGroups );
        }

        static boolean numberOfColumnsMatch( List<Map<String,Object>> rows )
        {
            return rows.stream().allMatch( row -> row.size() == EXPECTED_COLUMNS );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "result with " + (CORES + REPLICAS) + " rows and " + EXPECTED_COLUMNS + " columns" )
                    .appendText( "\n" ).appendText( "ids: " + expectedMemberIds )
                    .appendText( "\n" ).appendText( "addresses: " + expectedAddresses )
                    .appendText( "\n" ).appendText( "databases: " + expectedDatabases )
                    .appendText( "\n" ).appendText( "groups: " + expectedGroups );
        }
    }
}
