/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.assertEventualOverview;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.containsRole;
import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.RoleInfo.UNKNOWN;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
class CausalClusteringProceduresIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

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
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), this::invokeDbmsProcedures );
    }

    @Test
    void dbmsListQueriesShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), this::invokeDbmsListQueries );
    }

    @Test
    void dbmsClusterOverviewShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), this::invokeDbmsClusterOverview );
    }

    @Test
    void dbmsClusterOverviewShouldBeAvailableOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.allMembers(), this::invokeDbmsClusterOverview );
    }

    @Test
    void routingProcedureShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), this::invokeRoutingProcedure );
    }

    @Test
    void routingProcedureShouldBeAvailableOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.allMembers(), this::invokeRoutingProcedure );
    }

    @Test
    void legacyRoutingProcedureShouldBeAvailable()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.allMembers(), this::invokeLegacyRoutingProcedure );
    }

    @Test
    void legacyRoutingProcedureShouldBeAvailableOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.allMembers(), this::invokeLegacyRoutingProcedure );
    }

    @Test
    void installedProtocolsProcedure()
    {
        verifyProcedureAvailability( DEFAULT_DATABASE_NAME, cluster.coreMembers(), this::invokeClusterProtocolsProcedure );
    }

    @Test
    void installedProtocolsProcedureOnSystemDatabase()
    {
        verifyProcedureAvailability( SYSTEM_DATABASE_NAME, cluster.coreMembers(), this::invokeClusterProtocolsProcedure );
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
            assertEventually( roleReportedByProcedure( leader, databaseName ), equalityCondition( RoleInfo.FOLLOWER ), 2, MINUTES );
        }
        finally
        {
            // restart the follower so cluster has the same shape as before this test
            follower.start();

            // await until follower views the correct cluster
            assertEventualOverview( new HamcrestCondition<>( allOf(
                    containsRole( LEADER, databaseName, 1 ),
                    containsRole( FOLLOWER, databaseName, 1 ) ) ), follower );
        }
    }

    @Test
    void clusterRoleProcedureForStoppedDatabase() throws Exception
    {
        var databaseName = "stoppedDatabase";
        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        verifyClusterRoleProcedure( databaseName );

        stopDatabase( databaseName, cluster );
        assertDatabaseEventuallyStopped( databaseName, cluster );

        assertRoleProcedureThrowsOnAllMembers( databaseName, Status.Database.DatabaseUnavailable );

        startDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        verifyClusterRoleProcedure( databaseName );
    }

    @Test
    void clusterRoleProcedureForDroppedDatabase() throws Exception
    {
        var databaseName = "droppedDatabase";
        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        verifyClusterRoleProcedure( databaseName );

        dropDatabase( databaseName, cluster, false );
        assertDatabaseEventuallyDoesNotExist( databaseName, cluster );

        assertRoleProcedureThrowsOnAllMembers( databaseName, Status.Database.DatabaseNotFound );
    }

    @Test
    void clusterRoleProcedureForNonExistingDatabase()
    {
        var databaseName = "nonExistingDatabase";

        assertRoleProcedureThrowsOnAllMembers( databaseName, Status.Database.DatabaseNotFound );
    }

    private static void verifyProcedureAvailability( String databaseName, Set<? extends ClusterMember> members,
            Function<Transaction,Result> procedureExecutor )
    {
        for ( var member : members )
        {
            assertEventually( () -> execute( member, databaseName, procedureExecutor ), new HamcrestCondition<>( hasSize( greaterThanOrEqualTo( 1 ) ) ),
                    1, MINUTES );
        }
    }

    private static List<Map<String,Object>> execute( ClusterMember member, String databaseName, Function<Transaction,Result> procedureExecutor )
    {
        var db = member.database( databaseName );
        try ( var transaction = db.beginTx();
              var result = procedureExecutor.apply( transaction ) )
        {
            return Iterators.asList( result );
        }
        catch ( Exception e )
        {
            return List.of();
        }
    }

    private void verifyClusterRoleProcedure( String databaseName ) throws Exception
    {
        var leader = cluster.awaitLeader( databaseName );

        for ( var member : cluster.coreMembers() )
        {
            var expectedRole = Objects.equals( member, leader ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
            assertEventually( roleReportedByProcedure( member, databaseName ), equalityCondition( expectedRole ), 2, MINUTES );
        }

        for ( var member : cluster.readReplicas() )
        {
            var expectedRole = RoleInfo.READ_REPLICA;
            assertEventually( roleReportedByProcedure( member, databaseName ), equalityCondition( expectedRole ), 2, MINUTES );
        }
    }

    private void assertRoleProcedureThrowsOnAllMembers( String databaseName, Status expectedStatus )
    {
        for ( var member : cluster.allMembers() )
        {
            var error = assertThrows( QueryExecutionException.class, () -> invokeClusterRoleProcedure( member, databaseName ) );
            var rootCause = getRootCause( error );
            assertThat( rootCause, instanceOf( ProcedureException.class ) );
            assertEquals( expectedStatus, ((ProcedureException) rootCause).status() );
        }
    }

    private Callable<RoleInfo> roleReportedByProcedure( ClusterMember member, String databaseName )
    {
        return () ->
        {
            try
            {
                return invokeClusterRoleProcedure( member, databaseName );
            }
            catch ( Exception e )
            {
                return UNKNOWN;
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

    private RoleInfo invokeClusterRoleProcedure( ClusterMember member, String databaseName )
    {
        // invoke the procedure in a context of the system database which always exists
        var db = member.systemDatabase();
        try ( var tx = db.beginTx();
              var result = tx.execute( "CALL dbms.cluster.role($databaseName)", Map.of( "databaseName", databaseName ) ) )
        {
            return RoleInfo.valueOf( (String) Iterators.single( result ).get( "role" ) );
        }
    }
}
