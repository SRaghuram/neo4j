/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@TestInstance( PER_METHOD )
class LeaderTransferIT
{
    private static final String databaseFoo = "dbfoo";
    private static final String priFoo = "priofoo";
    private static final String databaseBar = "dbbar";
    private static final String prioBar = "priobar";
    private static final int BAR_MEMBER_ID = 2;
    private static final int FOO_MEMBER_ID = 1;

    @Inject
    private ClusterFactory clusterFactory;

    Cluster setUpCluster() throws ExecutionException, InterruptedException
    {
        var cluster = clusterFactory.createCluster(
                clusterConfig()
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( DEFAULT_DATABASE_NAME ).leadership_priority_group, "prio" )
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( SYSTEM_DATABASE_NAME ).leadership_priority_group, "prio" )
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( databaseFoo ).leadership_priority_group, priFoo )
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( databaseBar ).leadership_priority_group, prioBar )
                        .withNumberOfCoreMembers( 3 )
                        .withInstanceCoreParam( CausalClusteringSettings.server_groups, value ->
                        {
                            if ( value == FOO_MEMBER_ID )
                            {
                                // 1 is prioritised for foo
                                return priFoo;
                            }
                            if ( value == BAR_MEMBER_ID )
                            {
                                // 2 is prioritised for bar
                                return prioBar;
                            }
                            return "";
                        } )
                        .withSharedCoreParam( CausalClusteringInternalSettings.leader_transfer_interval, "5s" ) );

        cluster.start();
        return cluster;
    }

    @Test
    void shouldTransferLeadershipOfInitialDbsToNewMember() throws Exception
    {
        var cluster = setUpCluster();
        cluster.awaitLeader();
        var additionalCore = cluster.addCoreMemberWithIndex( 3 );
        additionalCore.config().set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) );

        additionalCore.start();

        assertLeaderIsOnCorrectMember( cluster, DEFAULT_DATABASE_NAME, additionalCore );
        assertLeaderIsOnCorrectMember( cluster, SYSTEM_DATABASE_NAME, additionalCore );
    }

    @Test
    void shouldTransferLeadershipToPrioritisedMemberWhenDatabaseHasBeenCreated() throws Exception
    {
        var cluster = setUpCluster();
        createDatabase( databaseFoo, cluster );
        createDatabase( databaseBar, cluster );

        assertDatabaseEventuallyStarted( databaseFoo, cluster );
        assertDatabaseEventuallyStarted( databaseBar, cluster );

        assertLeaderIsOnCorrectMember( cluster, databaseBar, cluster.getCoreMemberByIndex( BAR_MEMBER_ID ) );
        assertLeaderIsOnCorrectMember( cluster, databaseFoo, cluster.getCoreMemberByIndex( FOO_MEMBER_ID ) );
    }

    @Test
    void shouldTransferLeadershipToMemberInDefaultGroupIfNoPrioritisedGroupIsSet() throws Exception
    {
        // given
        var config = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( CausalClusteringSettings.default_leadership_priority_group, "default_prio" )
                .withSharedCoreParam( CausalClusteringInternalSettings.leader_transfer_interval, "5s" )
                .withSharedCoreParam( CausalClusteringSettings.leader_balancing, "equal_balancing" );
        var cluster = clusterFactory.createCluster( config );

        cluster.start();

        cluster.awaitLeader();

        var additionalCore = cluster.addCoreMemberWithIndex( 3 );
        additionalCore.config().set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "default_prio" ) );

        additionalCore.start();

        assertLeaderIsOnCorrectMember( cluster, DEFAULT_DATABASE_NAME, additionalCore );
        assertLeaderIsOnCorrectMember( cluster, SYSTEM_DATABASE_NAME, additionalCore );

        createDatabase( databaseFoo, cluster );

        assertDatabaseEventuallyStarted( databaseFoo, cluster );
        assertLeaderIsOnCorrectMember( cluster, databaseFoo, additionalCore );
    }

    @Test
    void shouldEventuallyBalanceLeadershipsToNewMembers() throws Exception
    {
        // given
        var config = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( new LeadershipPriorityGroupSetting( SYSTEM_DATABASE_NAME ).leadership_priority_group, "prio" )
                .withSharedCoreParam( CausalClusteringInternalSettings.leader_transfer_interval, "5s" )
                .withSharedCoreParam( CausalClusteringSettings.leader_balancing, "equal_balancing" );
        var cluster = clusterFactory.createCluster( config );
        cluster.start();

        var dbNames = IntStream.iterate( 0, i -> i + 1 )
                               .mapToObj( idx -> "database-" + idx )
                               .limit( 6 )
                               .collect( Collectors.toList() );

        for ( String dbName : dbNames )
        {
            createDatabase( dbName, cluster );
        }

        for ( String dbName : dbNames )
        {
            assertDatabaseEventuallyStarted( dbName, cluster );
        }

        // when
        var newMember = cluster.addCoreMemberWithIndex( 3 );
        newMember.start();

        Predicate<Boolean> identity = bool -> bool;
        // then
        assertEventually( "New member has leaderships", () -> hasAnyLeaderships( cluster, newMember, dbNames ),
                          identity, 3, TimeUnit.MINUTES );
    }

    private static void assertLeaderIsOnCorrectMember( Cluster cluster, String database, CoreClusterMember desiredLeader )
    {
        assertEventually( "leader is on correct member " + desiredLeader, () -> cluster.awaitLeader( database ),
                coreClusterMember -> coreClusterMember.serverId().equals( desiredLeader.serverId() ), 1, TimeUnit.MINUTES );
    }

    private static boolean hasAnyLeaderships( Cluster cluster, CoreClusterMember member, List<String> dbNames )
    {
        return dbNames.stream()
                      .map( dbName -> cluster.getMemberWithAnyRole( dbName, Role.LEADER ) )
                      .anyMatch( m -> Objects.equals( member, m ) );
    }
}
