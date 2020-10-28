/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import com.neo4j.test.driver.DriverTestHelper;
import com.neo4j.test.driver.WaitResponseStates;
import com.neo4j.test.driver.WaitResponses;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasDropped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasStopped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterExtension
@DriverExtension
public class WaitDatabaseOperationIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private DriverFactory driverFactory;

    private Driver driver;
    private Cluster cluster;

    @BeforeAll
    void setUpClusterAndDriver() throws IOException, ExecutionException, InterruptedException
    {
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        cluster.start();
        driver = driverFactory.graphDatabaseDriver( cluster );
    }

    private static OperationFunction[] blockingOperations()
    {
        return new OperationFunction[]{
                DriverTestHelper::createDatabaseWait,
                DriverTestHelper::startDatabaseWait,
                DriverTestHelper::stopDatabaseWait,
                DriverTestHelper::dropDatabaseWait};
    }

    @ParameterizedTest
    @MethodSource( "blockingOperations" )
    void testWaitCommands( OperationFunction operationFunction )
    {
        var responses = operationFunction.apply( driver, "foo" );
        assertResponsesAreSuccessful( responses );
        var expectedSize = cluster.readReplicas().size() + cluster.coreMembers().size();
        assertThat( responses.responses() ).hasSize( expectedSize );
    }

    @Test
    void createDatabaseIfNotExistsShouldWork()
    {
        var dbName = "ifNotExistsDb";
        try ( Session session = driver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            var responses = session.writeTransaction( tx -> WaitResponses.create( tx.run( "CREATE DATABASE " + dbName + " IF NOT EXISTS WAIT" ) ) );

            assertResponsesAreSuccessful( responses );
            assertDatabaseHasStarted( dbName, cluster );

            var responsesAfter = session.writeTransaction( tx -> WaitResponses.create( tx.run( "CREATE DATABASE " + dbName + " IF NOT EXISTS WAIT" ) ) );

            assertResponsesAreSuccessful( responsesAfter );
            assertThat( responsesAfter.responses() )
                    .extracting( "message" )
                    .containsExactly( "No operation needed" );
        }
    }

    @Test
    void dropDatabaseIfExistsShouldWork()
    {
        var dbName = "ifExistsDb";
        try ( Session session = driver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            var responses = session.writeTransaction( tx -> WaitResponses.create( tx.run( "DROP DATABASE " + dbName + " IF EXISTS WAIT" ) ) );

            assertResponsesAreSuccessful( responses );
            assertThat( responses.responses() )
                    .extracting( "message" )
                    .containsExactly( "No operation needed" );
        }
    }

    @Test
    void shouldHandleOrReplace()
    {
        var dbName = "orReplaceDb";
        try ( Session session = driver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            var responses = session.writeTransaction( tx -> WaitResponses.create( tx.run( "CREATE OR REPLACE DATABASE " + dbName + " WAIT" ) ) );

            assertResponsesAreSuccessful( responses );
            assertDatabaseHasStarted( dbName, cluster );
            var idBefore = cluster.getCoreMemberByIndex( 0 ).databaseId( dbName );

            var responsesAfter = session.writeTransaction( tx -> WaitResponses.create( tx.run( "CREATE OR REPLACE DATABASE " + dbName + " WAIT" ) ) );

            var idAfter = cluster.getCoreMemberByIndex( 0 ).databaseId( dbName );
            assertResponsesAreSuccessful( responsesAfter );
            assertDatabaseHasStarted( dbName, cluster );
            assertThat( idAfter ).isNotEqualTo( idBefore );
        }
    }

    @Test
    void shouldBeAvailableAfterBlockingCreate()
    {
        var dbName = "createDB";
        var responses = DriverTestHelper.createDatabaseWait( driver, dbName );

        assertResponsesAreSuccessful( responses );
        assertDatabaseHasStarted( dbName, cluster );
        DriverTestHelper.writeData( driver, dbName );
    }

    @Test
    void shouldBeShutdownAfterBlockingStop()
    {
        var dbName = "stopDb";
        DriverTestHelper.createDatabaseNoWait( driver, dbName );
        var responses = DriverTestHelper.stopDatabaseWait( driver, dbName );

        assertResponsesAreSuccessful( responses );
        assertDatabaseHasStopped( dbName, cluster );
        assertThrows( Exception.class, () -> DriverTestHelper.writeData( driver, dbName ) );
    }

    @Test
    void shouldBeAvailableAfterBlockingStart()
    {
        var dbName = "baz";
        DriverTestHelper.createDatabaseNoWait( driver, dbName );
        DriverTestHelper.stopDatabaseNoWait( driver, dbName );
        var responses = DriverTestHelper.startDatabaseWait( driver, dbName );

        assertResponsesAreSuccessful( responses );
        assertDatabaseHasStarted( dbName, cluster );
        DriverTestHelper.writeData( driver, dbName );
    }

    @Test
    void shouldBeGoneAfterBlockingDrop()
    {
        var dbName = "stopDb";
        DriverTestHelper.createDatabaseNoWait( driver, dbName );
        var responses = DriverTestHelper.dropDatabaseWait( driver, dbName );

        assertResponsesAreSuccessful( responses );
        assertDatabaseHasDropped( dbName, cluster );
        assertThrows( Exception.class, () -> DriverTestHelper.writeData( driver, dbName ) );
    }

    private void assertResponsesAreSuccessful( WaitResponses responses )
    {
        assertThat( responses.responses() )
                .extracting( "state" )
                .containsOnly( WaitResponseStates.CaughtUp );
    }

    private interface OperationFunction extends BiFunction<Driver,String,WaitResponses>
    {
    }
}
