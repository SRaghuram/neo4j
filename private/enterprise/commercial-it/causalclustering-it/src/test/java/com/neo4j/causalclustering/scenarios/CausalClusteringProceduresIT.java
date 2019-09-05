/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.ClusterOverviewHelper.assertEventualOverview;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.containsRole;
import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class CausalClusteringProceduresIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    @BeforeAll
    void setup() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 2 )
                .withNumberOfReadReplicas( 1 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void dbmsProceduresShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeDbmsProcedures( tx ) );
    }

    @Test
    void dbmsListQueriesShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeDbmsListQueries( tx ) );
    }

    @Test
    void dbmsClusterOverviewShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeDbmsClusterOverview( tx ) );
    }

    @Test
    void dbmsClusterOverviewShouldBeAvailableOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeDbmsClusterOverview( tx ) );
    }

    @Test
    void routingProcedureShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeRoutingProcedure( tx ) );
    }

    @Test
    void routingProcedureShouldBeAvailableOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeRoutingProcedure( tx ) );
    }

    @Test
    void legacyRoutingProcedureShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeLegacyRoutingProcedure( tx ) );
    }

    @Test
    void legacyRoutingProcedureShouldBeAvailableOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.allMembers(), ( db, tx ) -> invokeLegacyRoutingProcedure( tx ) );
    }

    @Test
    void installedProtocolsProcedure()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.coreMembers(), ( db, tx ) -> invokeClusterProtocolsProcedure( tx ) );
    }

    @Test
    void installedProtocolsProcedureOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.coreMembers(), ( db, tx ) -> invokeClusterProtocolsProcedure( tx ) );
    }

    @Test
    void clusterRoleProcedureShouldBeAvailable() throws Exception
    {
        verifyClusterRoleProcedure( DEFAULT_DATABASE_NAME );
    }

    @Test
    void clusterRoleProcedureShouldBeAvailableOnSystemDatabase() throws Exception
    {
        verifyClusterRoleProcedure( SYSTEM_DATABASE_NAME );
    }

    @Test
    void clusterRoleProcedureAfterFollowerShutdown() throws Exception
    {
        var databaseName = DEFAULT_DATABASE_NAME;
        var leader = cluster.awaitLeader();
        var follower = cluster.getMemberWithAnyRole( databaseName, Role.FOLLOWER );

        assertThat( cluster.coreMembers(), hasSize( 2 ) );

        try
        {
            // shutdown the only follower and wait for the leader to become a follower
            follower.shutdown();
            assertEventually( roleReportedByProcedure( leader, databaseName ), equalTo( RoleInfo.FOLLOWER ), 2, MINUTES );
        }
        finally
        {
            // restart the follower so cluster has the same shape as before this test
            follower.start();

            // await until follower views the correct cluster
            assertEventualOverview( allOf(
                    containsRole( LEADER, databaseName, 1 ),
                    containsRole( FOLLOWER, databaseName, 1 ) ), follower );
        }
    }

    private static void verifyProcedureAvailability( String databaseName, Set<? extends ClusterMember> members,
            BiFunction<GraphDatabaseService,Transaction,Result> procedureExecutor )
    {
        for ( var member : members )
        {
            var db = member.database( databaseName );
            try ( var transaction = db.beginTx() )
            {
                try ( var result = procedureExecutor.apply( db, transaction ) )
                {
                    var records = Iterators.asList( result );
                    assertThat( records, hasSize( greaterThanOrEqualTo( 1 ) ) );
                }
            }
        }
    }

    private void verifyClusterRoleProcedure( String databaseName ) throws Exception
    {
        var leader = cluster.awaitLeader( databaseName );

        for ( var member : cluster.coreMembers() )
        {
            var expectedRole = Objects.equals( member, leader ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
            assertEventually( roleReportedByProcedure( member, databaseName ), equalTo( expectedRole ), 2, MINUTES );
        }

        for ( var member : cluster.readReplicas() )
        {
            var expectedRole = RoleInfo.READ_REPLICA;
            assertEventually( roleReportedByProcedure( member, databaseName ), equalTo( expectedRole ), 2, MINUTES );
        }
    }

    private ThrowingSupplier<RoleInfo,RuntimeException> roleReportedByProcedure( ClusterMember member, String databaseName )
    {
        return () ->
        {
            var db = member.database( databaseName );
            try ( var transaction = db.beginTx() )
            {
                try ( var result = invokeClusterRoleProcedure( transaction, databaseName ) )
                {
                    return RoleInfo.valueOf( (String) Iterators.single( result ).get( "role" ) );
                }
            }
        };
    }

    private Result invokeDbmsProcedures( Transaction tx )
    {
        return tx.execute( "CALL dbms.procedures()" );
    }

    private Result invokeDbmsListQueries( Transaction tx )
    {
        return tx.execute( "CALL dbms.listQueries()" );
    }

    private Result invokeDbmsClusterOverview( Transaction tx )
    {
        return tx.execute( "CALL dbms.cluster.overview()" );
    }

    private Result invokeRoutingProcedure( Transaction tx )
    {
        return tx.execute( "CALL dbms.routing.getRoutingTable($routingContext)", Map.of( "routingContext", emptyMap() ) );
    }

    private Result invokeLegacyRoutingProcedure( Transaction tx )
    {
        return tx.execute( "CALL dbms.cluster.routing.getRoutingTable($routingContext)", Map.of( "routingContext", emptyMap() ) );
    }

    private Result invokeClusterProtocolsProcedure( Transaction tx )
    {
        return tx.execute( "CALL dbms.cluster.protocols()" );
    }

    private Result invokeClusterRoleProcedure( Transaction tx, String databaseName )
    {
        return tx.execute( "CALL dbms.cluster.role($databaseName)", Map.of( "databaseName", databaseName ) );
    }
}
