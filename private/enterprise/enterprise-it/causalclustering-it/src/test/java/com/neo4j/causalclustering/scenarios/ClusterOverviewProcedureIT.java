/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.listDatabases;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.configuration.CausalClusteringSettings.server_groups;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class ClusterOverviewProcedureIT
{
    private static final Set<String> defaultDatabases = Set.of( SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME );

    private static final String CORE_GROUP = "core";
    private static final String READ_REPLICA_GROUP = "replica";
    private static final String EU_GROUP_PREFIX = "eu-";
    private static final String US_GROUP_PREFIX = "us-";

    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    @BeforeAll
    static void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( buildClusterConfig() );
        cluster.start();
    }

    @BeforeEach
    void dropNonDefaultDatabases() throws Exception
    {
        for ( var databaseName : listDatabases( cluster ) )
        {
            if ( !defaultDatabases.contains( databaseName ) )
            {
                dropDatabase( databaseName, cluster );
            }
        }
    }

    @Test
    void shouldReturnClusterOverview()
    {
        var clusterOverviewMatcher = new ClusterOverviewMatcher( cluster, defaultDatabases );

        awaitClusterOverviewToMatch( clusterOverviewMatcher );
    }

    @Test
    void shouldReturnClusterOverviewForRestartedDatabase() throws Exception
    {
        var newDatabaseName = "foo";
        var clusterOverviewWithoutNewDatabase = new ClusterOverviewMatcher( cluster, defaultDatabases );
        var clusterOverviewWitNewDatabase = new ClusterOverviewMatcher( cluster, Set.of( SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME, newDatabaseName ) );

        awaitClusterOverviewToMatch( clusterOverviewWithoutNewDatabase );

        createDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyStarted( newDatabaseName, cluster );
        awaitClusterOverviewToMatch( clusterOverviewWitNewDatabase );

        stopDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyStopped( newDatabaseName, cluster );
        awaitClusterOverviewToMatch( clusterOverviewWithoutNewDatabase );

        startDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyStarted( newDatabaseName, cluster );
        awaitClusterOverviewToMatch( clusterOverviewWitNewDatabase );
    }

    private static void awaitClusterOverviewToMatch( ClusterOverviewMatcher matcher )
    {
        for ( var member : cluster.allMembers() )
        {
            assertEventually( () -> invokeClusterOverviewProcedure( member ), new HamcrestCondition<>( matcher ), 2, MINUTES );
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

    private static ClusterConfig buildClusterConfig()
    {
        var disableBoltAndBackup = Map.of( BoltConnector.enabled.name(), FALSE, online_backup_enabled.name(), FALSE );
        return clusterConfig()
                .withSharedCoreParams( disableBoltAndBackup )
                .withSharedReadReplicaParams( disableBoltAndBackup )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withInstanceCoreParam( server_groups, id -> CORE_GROUP + "," + EU_GROUP_PREFIX + id )
                .withInstanceReadReplicaParam( server_groups, id -> READ_REPLICA_GROUP + "," + US_GROUP_PREFIX + id );
    }

    private static class ClusterOverviewMatcher extends TypeSafeMatcher<List<Map<String,Object>>>
    {
        static final int EXPECTED_COLUMNS = 4;

        final Cluster cluster;
        final Set<String> databaseNames;

        ClusterOverviewMatcher( Cluster cluster, Set<String> databaseNames )
        {
            this.cluster = cluster;
            this.databaseNames = databaseNames;
        }

        @Override
        protected boolean matchesSafely( List<Map<String,Object>> rows )
        {
            if ( rows.size() != cluster.allMembers().size() )
            {
                return false;
            }

            return numberOfColumnsMatch( rows ) &&
                   memberIdsMatch( rows ) &&
                   addressesMatch( rows ) &&
                   databasesMatch( rows ) &&
                   groupsMatch( rows );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "result with " + cluster.allMembers().size() + " rows and " + EXPECTED_COLUMNS + " columns" )
                    .appendText( "\n" ).appendText( "ids: " + expectedMemberIds() )
                    .appendText( "\n" ).appendText( "addresses: " + expectedAddresses() )
                    .appendText( "\n" ).appendText( "databases: " + expectedDatabases() )
                    .appendText( "\n" ).appendText( "groups: " + expectedGroups() );
        }

        static boolean numberOfColumnsMatch( List<Map<String,Object>> rows )
        {
            return rows.stream().allMatch( row -> row.size() == EXPECTED_COLUMNS );
        }

        boolean memberIdsMatch( List<Map<String,Object>> rows )
        {
            var expectedMemberIds = expectedMemberIds();

            var actualMemberIds = rows.stream()
                    .map( row -> (String) row.get( "id" ) )
                    .collect( toList() );

            return Objects.equals( expectedMemberIds, actualMemberIds );
        }

        @SuppressWarnings( "unchecked" )
        boolean addressesMatch( List<Map<String,Object>> rows )
        {
            var expectedAddresses = expectedAddresses();

            var actualAddresses = rows.stream()
                    .map( row -> (List<String>) row.get( "addresses" ) )
                    .collect( toList() );

            return Objects.equals( expectedAddresses, actualAddresses );
        }

        @SuppressWarnings( "unchecked" )
        boolean databasesMatch( List<Map<String,Object>> rows )
        {
            var expectedDatabases = expectedDatabases();

            var actualDatabases = rows.stream()
                    .map( row -> (Map<String,String>) row.get( "databases" ) )
                    .collect( toList() );

            return Objects.equals( expectedDatabases, actualDatabases );
        }

        @SuppressWarnings( "unchecked" )
        boolean groupsMatch( List<Map<String,Object>> rows )
        {
            var expectedGroups = expectedGroups();

            var actualGroups = rows.stream()
                    .map( row -> (List<String>) row.get( "groups" ) )
                    .collect( toList() );

            return Objects.equals( expectedGroups, actualGroups );
        }

        List<String> expectedMemberIds()
        {
            return sortedClusterMembers()
                    .map( member -> member.serverId().uuid().toString() )
                    .collect( toList() );
        }

        List<List<String>> expectedAddresses()
        {
            return sortedClusterMembers()
                    .map( ClusterMember::clientConnectorAddresses )
                    .map( ConnectorAddresses::publicUriList )
                    .collect( toList() );
        }

        List<Map<String,String>> expectedDatabases()
        {
            return sortedClusterMembers()
                    .map( member -> expectedDatabasesForMember( member, databaseNames ) )
                    .collect( toList() );
        }

        Map<String,String> expectedDatabasesForMember( ClusterMember member, Set<String> databaseNames )
        {
            return databaseNames.stream()
                    .collect( toMap( identity(), name -> expectedRole( member, name ).toString() ) );
        }

        List<List<String>> expectedGroups()
        {
            return sortedClusterMembers()
                    .map( this::expectedGroupsForMember )
                    .collect( toList() );
        }

        List<String> expectedGroupsForMember( ClusterMember member )
        {
            if ( isCore( member ) )
            {
                return List.of( CORE_GROUP, EU_GROUP_PREFIX + member.index() );
            }
            if ( isReadReplica( member ) )
            {
                return List.of( READ_REPLICA_GROUP, US_GROUP_PREFIX + member.index() );
            }
            throw new IllegalArgumentException( "Unable to find groups for " + member );
        }

        Stream<ClusterMember> sortedClusterMembers()
        {
            return cluster.allMembers()
                    .stream()
                    .sorted( comparing( member -> member.serverId().uuid() ) );
        }

        RoleInfo expectedRole( ClusterMember member, String databaseName )
        {
            if ( isLeader( member, databaseName ) )
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

        boolean isLeader( ClusterMember member, String databaseName )
        {
            var leader = cluster.getMemberWithAnyRole( databaseName, Role.LEADER );
            return Objects.equals( member, leader );
        }

        boolean isCore( ClusterMember member )
        {
            return cluster.coreMembers().contains( member );
        }

        boolean isReadReplica( ClusterMember member )
        {
            return cluster.readReplicas().contains( member );
        }
    }
}
