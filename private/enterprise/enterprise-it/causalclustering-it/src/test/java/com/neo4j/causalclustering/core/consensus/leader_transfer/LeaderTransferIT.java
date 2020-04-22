/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
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
    private Cluster cluster;

    @BeforeAll
    void setUp() throws ExecutionException, InterruptedException
    {
        cluster = clusterFactory.createCluster(
                clusterConfig()
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ).setting(), "prio" )
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ).setting(), "prio" )
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( databaseFoo ).setting(), priFoo )
                        .withSharedCoreParam( new LeadershipPriorityGroupSetting( databaseBar ).setting(), prioBar )
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
                        .withSharedCoreParam( CausalClusteringSettings.leader_transfer_interval, "5s" ) );

        cluster.start();
    }

    @Test
    void shouldTransferLeadershipOfInitialDbsToNewMember() throws TimeoutException
    {
        cluster.awaitLeader();
        var additionalCore = cluster.addCoreMemberWithId( 3 );
        additionalCore.config().set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) );

        additionalCore.start();

        assertLeaderIsOnCorrectMember( cluster, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, additionalCore );
        assertLeaderIsOnCorrectMember( cluster, GraphDatabaseSettings.SYSTEM_DATABASE_NAME, additionalCore );
    }

    @Test
    void shouldTransferLeadershipToPrioritisedMemberWhenDatabaseHasBeenCreated() throws Exception
    {
        createDatabase( databaseFoo, cluster );
        createDatabase( databaseBar, cluster );

        assertDatabaseEventuallyStarted( databaseFoo, cluster );
        assertDatabaseEventuallyStarted( databaseBar, cluster );

        assertLeaderIsOnCorrectMember( cluster, databaseBar, cluster.getCoreMemberById( BAR_MEMBER_ID ) );
        assertLeaderIsOnCorrectMember( cluster, databaseFoo, cluster.getCoreMemberById( FOO_MEMBER_ID ) );
    }

    private static void assertLeaderIsOnCorrectMember( Cluster cluster, String database, CoreClusterMember desiredLeader )
    {
        assertEventually( "leader is on correct member " + desiredLeader, () -> cluster.awaitLeader( database ),
                coreClusterMember -> coreClusterMember.id().equals( desiredLeader.id() ), 1, TimeUnit.MINUTES );
    }
}
