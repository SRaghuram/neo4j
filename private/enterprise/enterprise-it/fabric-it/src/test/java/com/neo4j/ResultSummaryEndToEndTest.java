/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.kernel.api.exceptions.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class ResultSummaryEndToEndTest
{
    private static Driver clientDriver;
    private static TestServer testServer;
    private static Neo4j shard0;
    private static Driver shard0Driver;
    private static DriverUtils driverUtils;

    @BeforeAll
    static void beforeAll()
    {
        shard0 = Neo4jBuilders.newInProcessBuilder().build();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard0.boltURI().toString(),
                "fabric.graph.0.name", "remote1",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                .setRaw( configProperties )
                .build();
        testServer = new TestServer( config );

        testServer.start();

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        shard0Driver = GraphDatabase.driver(
                shard0.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        driverUtils = new DriverUtils( "mega" );
    }

    @BeforeEach
    void beforeEach()
    {
        try ( Transaction tx = shard0Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume();
            tx.commit();
        }

        try ( Transaction tx = clientDriver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Person {name: 'Carrie', uid: 0, age: 50})" ).consume();
            tx.run( "CREATE (:Person {name: 'Dan',  uid: 1, age: 60})" ).consume();
            tx.commit();
        }
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close(),
                () -> shard0Driver.close(),
                () -> shard0.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testSingleLocalGraphExplain()
    {
        var query = joinAsLines( "EXPLAIN",
                "MATCH (n {name: 'Carrie'})",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 0 );
        assertTrue(resultSummary.hasPlan());
        assertFalse( resultSummary.hasProfile() );
        assertNotNull( resultSummary.plan() );

        var expectedPlan =
                plan( "ProduceResults@mega",
                        plan( "Filter@mega",
                                plan( "AllNodesScan@mega" )
                        )
                );

        expectedPlan.assertPlan( resultSummary.plan() );
    }

    @Test
    void testTaggingWithExplicitUseExplain()
    {
        var query = joinAsLines( "EXPLAIN",
                "USE neo4j",
                "MATCH (n {name: 'Carrie'} )",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 0 );
        assertTrue(resultSummary.hasPlan());
        assertNotNull( resultSummary.plan() );

        var expectedPlan =
                plan( "ProduceResults@neo4j",
                        plan( "Filter@neo4j",
                                plan( "AllNodesScan@neo4j" )
                        )
                );

        expectedPlan.assertPlan( resultSummary.plan() );
    }

    @Test
    void testSingleRemoteGraphExplain()
    {
        var query = joinAsLines( "EXPLAIN",
                "USE mega.remote1",
                "MATCH (n {name: 'Anna'} )",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 0 );
        assertTrue(resultSummary.hasPlan());
        assertNotNull( resultSummary.plan() );

        var expectedPlan =
                plan("RemoteExecution@graph(0)",
                        plan( "ProduceResults",
                                plan( "Filter",
                                        plan( "AllNodesScan" )
                                )
                        )
                );

        expectedPlan.assertPlan( resultSummary.plan() );
    }

    @Disabled( "Union is executed by fabric instead of Cypher runtime and therefore we get fabric plans." )
    // TODO: crate a union profile test when this is resolved
    @Test
    void testLocalGraphUnionExplain()
    {
        var query = joinAsLines( "EXPLAIN",
                "MATCH (n {name: 'Carrie'})",
                "RETURN n",
                "UNION",
                "MATCH (n {name: 'Dan'})",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 0 );
        assertTrue(resultSummary.hasPlan());
        assertFalse( resultSummary.hasProfile() );
        assertNotNull( resultSummary.plan() );

        var expectedPlan =
                plan( "ProduceResults@mega",
                        plan( "Distinct@mega",
                                plan( "Union@mega",
                                        plan( "Projection@mega",
                                                plan( "Filter@mega",
                                                        plan( "AllNodesScan@mega" )
                                                )
                                        ),
                                        plan( "Projection@mega",
                                                plan( "Filter@mega",
                                                        plan( "AllNodesScan@mega" )
                                                )
                                        )
                                )
                        )
                );

        expectedPlan.assertPlan( resultSummary.plan() );
    }

    @Test
    void testMultiGraphExplain()
    {
        var query = joinAsLines( "EXPLAIN",
                "UNWIND [0, 1] AS gid",
                "CALL {",
                "   USE mega.graph(gid)",
                "   MATCH (n)",
                "   RETURN n",
                "}",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 0 );
        assertTrue(resultSummary.hasPlan());
        assertNotNull( resultSummary.plan() );

        var expectedPlan =
                plan("Leaf",
                        plan( "Apply",
                                plan( "Leaf",
                                        plan( "Init" )
                                ),
                                plan( "Leaf",
                                        plan( "Init" )
                                )
                        )
                );

        expectedPlan.assertPlan( resultSummary.plan() );
    }

    @Test
    void testSingleLocalGraphProfile()
    {
        var query = joinAsLines( "PROFILE",
                "USE neo4j",
                "MATCH (n {name: 'Carrie'})",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 1 );
        assertTrue(resultSummary.hasPlan());
        assertTrue( resultSummary.hasProfile() );
        assertNotNull( resultSummary.plan() );
        assertNotNull( resultSummary.profile() );

        var expectedPlan =
                plan( "ProduceResults@neo4j",
                        plan( "Filter@neo4j",
                                plan( "AllNodesScan@neo4j" )
                        )
                );

        var profiledPlan = resultSummary.profile();
        expectedPlan.assertPlan( profiledPlan );

        var expectedStats =
                stats( 1, false,
                        stats( 1, true,
                                stats( 2, true )
                        )
                );

        expectedStats.assertStats( profiledPlan );
    }

    @Test
    void testSingleRemoteGraphProfile()
    {
        var query = joinAsLines( "PROFILE",
                "USE mega.remote1",
                "MATCH (n {name: 'Anna'} )",
                "RETURN n" );

        var resultSummary = runAndGetSummary( query, 1 );
        assertTrue(resultSummary.hasPlan());
        assertNotNull( resultSummary.plan() );
        assertNotNull( resultSummary.plan() );
        assertNotNull( resultSummary.profile() );

        var expectedPlan =
                plan("RemoteExecution@graph(0)",
                        plan( "ProduceResults",
                                plan( "Filter",
                                        plan( "AllNodesScan" )
                                )
                        )
                );

        var profiledPlan = resultSummary.profile();
        expectedPlan.assertPlan( profiledPlan );

        var expectedStats =
                stats( 1, false,
                        stats( 1, false,
                                stats( 1, true,
                                        stats( 2, true )
                                )
                        )
                );

        expectedStats.assertStats( profiledPlan );
    }

    @Test
    void testMultiGraphProfile()
    {
        var query = joinAsLines( "PROFILE",
                "UNWIND [0, 1] AS gid",
                "CALL {",
                "   USE mega.graph(gid)",
                "   MATCH (n)",
                "   RETURN n",
                "}",
                "RETURN n" );

        var exception = assertThrows( ClientException.class, () -> runAndGetSummary( query, 0 ) );
        assertEquals( "'PROFILE' not supported for multi graph queries", exception.getMessage() );
    }

    @Test
    void testLocalGraphStatistics()
    {
        var query = joinAsLines(
                "USE neo4j",
                "CREATE (n:Person {name: 'John Doe'})",
                "SET n.uid = 99"
        );

        var resultSummary = runAndGetSummary( query, 0 );
        var counters = resultSummary.counters();
        assertNotNull( counters );
        assertTrue( counters.containsUpdates() );
        assertFalse( counters.containsSystemUpdates() );
        assertEquals( 1, counters.nodesCreated() );
        assertEquals( 0, counters.nodesDeleted() );
        assertEquals( 0, counters.relationshipsCreated() );
        assertEquals( 0, counters.relationshipsDeleted() );
        assertEquals( 2, counters.propertiesSet() );
        assertEquals( 1, counters.labelsAdded() );
        assertEquals( 0, counters.labelsRemoved() );
        assertEquals( 0, counters.indexesAdded() );
        assertEquals( 0, counters.indexesRemoved() );
        assertEquals( 0, counters.constraintsAdded() );
        assertEquals( 0, counters.constraintsRemoved() );
        assertEquals( 0, counters.systemUpdates() );
    }

    @Test
    void testRemoteGraphStatistics()
    {
        var query = joinAsLines(
                "USE mega.remote1",
                "CREATE (n:Person {name: 'John Doe'})",
                "SET n:Friend"
        );

        var resultSummary = runAndGetSummary( query, 0 );
        var counters = resultSummary.counters();
        assertNotNull( counters );
        assertTrue( counters.containsUpdates() );
        assertFalse( counters.containsSystemUpdates() );
        assertEquals( 1, counters.nodesCreated() );
        assertEquals( 0, counters.nodesDeleted() );
        assertEquals( 0, counters.relationshipsCreated() );
        assertEquals( 0, counters.relationshipsDeleted() );
        assertEquals( 1, counters.propertiesSet() );
        assertEquals( 2, counters.labelsAdded() );
        assertEquals( 0, counters.labelsRemoved() );
        assertEquals( 0, counters.indexesAdded() );
        assertEquals( 0, counters.indexesRemoved() );
        assertEquals( 0, counters.constraintsAdded() );
        assertEquals( 0, counters.constraintsRemoved() );
        assertEquals( 0, counters.systemUpdates() );
    }

    @Test
    void testMultiGraphStatistics()
    {
        var query = joinAsLines(
                "UNWIND [1, 2, 3] AS x",
                "CALL {",
                "  WITH x",
                "  USE mega.graph(0)",
                "  CREATE (n:T {p: x})",
                "  RETURN n",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  MATCH (m:T {p: 1})",
                "  CREATE (m)-[r:R]->(x:X)",
                "  SET x:Y, x.y = 10",
                "  REMOVE x:Y",
                "  REMOVE x.y",
                "  DETACH DELETE m",
                "  RETURN m",
                "}",
                "RETURN x"
        );

        var resultSummary = runAndGetSummary( query, 1 );

        assertThat( resultSummary.queryType() ).isEqualTo( QueryType.READ_WRITE );
        assertThat( resultSummary.counters().containsUpdates() ).isEqualTo( true );
        assertThat( resultSummary.counters().nodesCreated() ).isEqualTo( 4 );
        assertThat( resultSummary.counters().nodesDeleted() ).isEqualTo( 1 );
        assertThat( resultSummary.counters().relationshipsCreated() ).isEqualTo( 1 );
        assertThat( resultSummary.counters().relationshipsDeleted() ).isEqualTo( 1 );
        assertThat( resultSummary.counters().propertiesSet() ).isEqualTo( 5 );
        assertThat( resultSummary.counters().labelsAdded() ).isEqualTo( 5 );
        assertThat( resultSummary.counters().labelsRemoved() ).isEqualTo( 1 );
        assertThat( resultSummary.counters().indexesAdded() ).isEqualTo( 0 );
        assertThat( resultSummary.counters().indexesRemoved() ).isEqualTo( 0 );
        assertThat( resultSummary.counters().constraintsAdded() ).isEqualTo( 0 );
        assertThat( resultSummary.counters().constraintsRemoved() ).isEqualTo( 0 );
    }

    @Test
    void testLocalGraphNotifications()
    {
        var query = joinAsLines(
                "EXPLAIN",
                "USE neo4j",
                "MATCH (n:NonExistentLabel)",
                "RETURN n"
        );

        var resultSummary = runAndGetSummary( query, 0 );
        assertEquals( 1, resultSummary.notifications().size() );
        assertThat( resultSummary.notifications().get( 0 ).code() ).contains( Status.Statement.UnknownLabelWarning.code().serialize() );
    }

    @Test
    void testRemoteGraphNotifications()
    {
        var query = joinAsLines(
                "EXPLAIN",
                "USE mega.remote1",
                "MATCH (n:NonExistentLabel)",
                "RETURN n"
        );

        var resultSummary = runAndGetSummary( query, 0 );
        assertEquals( 1, resultSummary.notifications().size() );
        assertThat( resultSummary.notifications().get( 0 ).code() ).contains( Status.Statement.UnknownLabelWarning.code().serialize() );
    }

    private ResultSummary runAndGetSummary( String query, int expectedNumberOfRecords )
    {
        return driverUtils.inSession( clientDriver, session -> {
            var result = session.run( query );
            assertEquals( expectedNumberOfRecords, result.list().size() );
            return result.consume();
        } );
    }

    private ProfileStats stats( int numRecords, boolean hasDbHits, ProfileStats... children )
    {
        return new ProfileStats( numRecords, hasDbHits, Arrays.asList( children ) );
    }

    private static class ProfileStats
    {
        private final int numRecords;
        private final boolean hasDbHits;
        private final List<ProfileStats> children;

        ProfileStats( int numRecords, boolean hasDbHits, List<ProfileStats> children )
        {
            this.numRecords = numRecords;
            this.hasDbHits = hasDbHits;
            this.children = children;
        }

        void assertStats( ProfiledPlan profiledPlan )
        {
            assertEquals( numRecords, profiledPlan.records() );
            assertEquals( hasDbHits, profiledPlan.dbHits() > 0 );

            assertEquals( children.size(), profiledPlan.children().size() );

            IntStream.range( 0, children.size() ).forEach( i -> children.get( i ).assertStats( profiledPlan.children().get( i ) ) );
        }
    }

    private ExpectedPlan plan( String operatorType, ExpectedPlan... children )
    {
        return new ExpectedPlan( operatorType, Arrays.asList( children ) );
    }

    private static class ExpectedPlan
    {

        private final String operatorType;
        private final List<ExpectedPlan> children;

        ExpectedPlan( String operatorType, List<ExpectedPlan> children )
        {
            this.operatorType = operatorType;
            this.children = children;
        }

        void assertPlan( Plan driverPlan )
        {
            assertEquals( operatorType, driverPlan.operatorType() );
            assertEquals( children.size(), driverPlan.children().size() );

            IntStream.range( 0, children.size() ).forEach( i -> children.get( i ).assertPlan( driverPlan.children().get( i ) ) );
        }
    }
}
