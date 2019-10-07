/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.PartialSummary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FabricExecutorTest
{

    private final PortUtils.Ports ports = PortUtils.findFreePorts();

    private final Map<String,String> configProperties = Map.of(
            "fabric.database.name", "mega",
            "fabric.graph.0.uri", "bolt://localhost:1111",
            "fabric.graph.1.uri", "bolt://localhost:2222",
            "fabric.routing.servers", "localhost:" + ports.bolt,
            "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
            "dbms.connector.bolt.enabled", "true"
    );

    private final Config config = Config.newBuilder()
            .setRaw( configProperties )
            .build();

    private TestServer testServer;
    private Driver clientDriver;
    private ArgumentCaptor<org.neo4j.bolt.runtime.AccessMode> accessModeArgument = ArgumentCaptor.forClass( org.neo4j.bolt.runtime.AccessMode.class );
    private StatementResult graph0Result;
    private StatementResult graph1Result;

    StatementResult createMockResult()
    {
        StatementResult mockStatementResult = mock( StatementResult.class );
        when( mockStatementResult.columns() ).thenReturn( Flux.fromIterable( List.of( "a", "b" ) ) );
        when( mockStatementResult.records() ).thenReturn( Flux.empty() );
        when( mockStatementResult.summary() ).thenReturn( Mono.empty() );
        return mockStatementResult;
    }

    PooledDriver createMockDriver( StatementResult mockStatementResult )
    {
        PooledDriver mockDriver = mock( PooledDriver.class );

        FabricDriverTransaction tx = mock( FabricDriverTransaction.class );

        when( tx.run( any(), any() ) ).thenReturn( mockStatementResult );
        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        when( mockDriver.beginTransaction( any(), accessModeArgument.capture(), any() ) ).thenReturn( Mono.just( tx ) );
        return mockDriver;
    }

    DriverPool createMockDriverPool( PooledDriver graph0, PooledDriver graph1 )
    {
        DriverPool pool = mock( DriverPool.class );
        doReturn( graph0 ).when( pool ).getDriver( argThat( g -> g.getId() == 0 ), any() );
        doReturn( graph1 ).when( pool ).getDriver( argThat( g -> g.getId() == 1 ), any() );
        return pool;
    }

    @BeforeEach
    void setUp()
    {
        testServer = new TestServer( config );
        graph0Result = createMockResult();
        graph1Result = createMockResult();

        testServer.addMocks( createMockDriverPool( createMockDriver( graph0Result ), createMockDriver( graph1Result ) ) );
        testServer.start();

        clientDriver = GraphDatabase.driver(
                "bolt://localhost:" + ports.bolt,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    @AfterEach
    void tearDown()
    {
        testServer.stop();
        clientDriver.close();
    }

    private Transaction transaction( String database, AccessMode mode )
    {
        return clientDriver.session( SessionConfig.builder().withDatabase( database ).withDefaultAccessMode( mode ).build() ).beginTransaction();
    }

    @Test
    void testReadInReadSession()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.success();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testReadInWriteSession()
    {
        Transaction tx = transaction( "mega", AccessMode.WRITE );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.success();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testWriteInReadSession()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.success();

        verifySessionConfig( 1, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testWriteInWriteSession()
    {
        Transaction tx = transaction( "mega", AccessMode.WRITE );
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();

        verifySessionConfig( 1, org.neo4j.bolt.runtime.AccessMode.WRITE );

        assertThrows( DatabaseException.class,
                () -> tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume()
        );
    }

    @Test
    void testMixedInReadSession()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testMixedInWriteSession()
    {
        Transaction tx = transaction( "mega", AccessMode.WRITE );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.WRITE );

        assertThrows( DatabaseException.class,
                () -> tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume()
        );
    }

    @Test
    void testLocalReturnQuery()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        List<Record> list = tx.run( String.join( "\n",
                "RETURN 1 AS x"
        ) ).list();
        tx.success();

        assertEquals( list.get( 0 ).get( "x" ).asLong(), 1 );
    }

    @Test
    void testLocalFlatCompositeQuery()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        List<Record> list = tx.run( String.join( "\n",
                "UNWIND [1, 2] AS x",
                "CALL { RETURN 'y' AS y }",
                "CALL { UNWIND [8, 9] AS z RETURN z }",
                "RETURN x, y, z ORDER BY x, y, z"
        ) ).list();
        tx.success();

        assertEquals( list.get( 0 ).get( "x" ).asLong(), 1 );
        assertEquals( list.get( 0 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 0 ).get( "z" ).asLong(), 8 );

        assertEquals( list.get( 1 ).get( "x" ).asLong(), 1 );
        assertEquals( list.get( 1 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 1 ).get( "z" ).asLong(), 9 );

        assertEquals( list.get( 2 ).get( "x" ).asLong(), 2 );
        assertEquals( list.get( 2 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 2 ).get( "z" ).asLong(), 8 );

        assertEquals( list.get( 3 ).get( "x" ).asLong(), 2 );
        assertEquals( list.get( 3 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 3 ).get( "z" ).asLong(), 9 );
    }

    @Test
    void testRemoteFlatCompositeQuery()
    {
        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" ) ), rec( Values.stringValue( "b" ) ) )
        );

        when( graph1Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "k" ) ), rec( Values.stringValue( "l" ) ) )
        );

        Transaction tx = transaction( "mega", AccessMode.READ );
        List<Record> list = tx.run( String.join( "\n",
                "UNWIND [0, 1] AS s",
                "CALL { FROM mega.graph(s) RETURN '' AS y }",
                "RETURN s, y ORDER BY s, y"
        ) ).list();
        tx.success();

        assertEquals( list.get( 0 ).get( "s" ).asLong(), 0 );
        assertEquals( list.get( 0 ).get( "y" ).asString(), "a" );

        assertEquals( list.get( 1 ).get( "s" ).asLong(), 0 );
        assertEquals( list.get( 1 ).get( "y" ).asString(), "b" );

        assertEquals( list.get( 2 ).get( "s" ).asLong(), 1 );
        assertEquals( list.get( 2 ).get( "y" ).asString(), "k" );

        assertEquals( list.get( 3 ).get( "s" ).asLong(), 1 );
        assertEquals( list.get( 3 ).get( "y" ).asString(), "l" );
    }

    @Test
    void testSummaryAggregation()
    {
        QueryStatistics stats0 = mock( QueryStatistics.class );
        when( stats0.getNodesCreated() ).thenReturn( 1 );
        when( stats0.getNodesDeleted() ).thenReturn( 1 );
        when( stats0.getRelationshipsCreated() ).thenReturn( 1 );
        when( stats0.getRelationshipsDeleted() ).thenReturn( 1 );
        when( stats0.getPropertiesSet() ).thenReturn( 1 );
        when( stats0.getLabelsAdded() ).thenReturn( 1 );
        when( stats0.getLabelsRemoved() ).thenReturn( 1 );
        when( stats0.getIndexesAdded() ).thenReturn( 1 );
        when( stats0.getIndexesRemoved() ).thenReturn( 1 );
        when( stats0.getConstraintsAdded() ).thenReturn( 1 );
        when( stats0.getConstraintsRemoved() ).thenReturn( 1 );
        when( stats0.getSystemUpdates() ).thenReturn( 1 );
        when( stats0.containsUpdates() ).thenReturn( true );
        when( stats0.containsSystemUpdates() ).thenReturn( false );

        QueryStatistics stats1 = mock( QueryStatistics.class );
        when( stats1.getNodesCreated() ).thenReturn( 2 );
        when( stats1.getNodesDeleted() ).thenReturn( 2 );
        when( stats1.getRelationshipsCreated() ).thenReturn( 2 );
        when( stats1.getRelationshipsDeleted() ).thenReturn( 2 );
        when( stats1.getPropertiesSet() ).thenReturn( 2 );
        when( stats1.getLabelsAdded() ).thenReturn( 2 );
        when( stats1.getLabelsRemoved() ).thenReturn( 2 );
        when( stats1.getIndexesAdded() ).thenReturn( 2 );
        when( stats1.getIndexesRemoved() ).thenReturn( 2 );
        when( stats1.getConstraintsAdded() ).thenReturn( 2 );
        when( stats1.getConstraintsRemoved() ).thenReturn( 2 );
        when( stats1.getSystemUpdates() ).thenReturn( 2 );
        when( stats1.containsUpdates() ).thenReturn( false );
        when( stats1.containsSystemUpdates() ).thenReturn( true );

        when( graph0Result.summary() ).thenReturn( Mono.just( new PartialSummary(
                stats0, null,
                List.of( NotificationCode.DEPRECATED_PROCEDURE.notification( new InputPosition( 100, 10, 5 ) ) )
        ) ) );

        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.of( null ) ) )
        );

        when( graph1Result.summary() ).thenReturn( Mono.just( new PartialSummary(
                stats1, null,
                List.of( NotificationCode.RUNTIME_UNSUPPORTED.notification( new InputPosition( 1, 1, 1 ) ) )
        ) ) );

        Transaction tx = transaction( "mega", AccessMode.READ );
        ResultSummary summary = tx.run( String.join( "\n",
                "CALL { FROM mega.graph(0) RETURN 1 AS x }",
                "CALL { FROM mega.graph(1) CREATE () RETURN 1 AS y }",
                "RETURN 1"
        ) ).summary();
        tx.success();

        assertThat( summary.statementType(), is( StatementType.READ_WRITE ) );

        assertThat( summary.hasPlan(), is( false ) );
        assertThat( summary.hasProfile(), is( false ) );

        assertThat( summary.counters().nodesCreated(), is( 3 ) );
        assertThat( summary.counters().nodesDeleted(), is( 3 ) );
        assertThat( summary.counters().relationshipsCreated(), is( 3 ) );
        assertThat( summary.counters().relationshipsDeleted(), is( 3 ) );
        assertThat( summary.counters().propertiesSet(), is( 3 ) );
        assertThat( summary.counters().labelsAdded(), is( 3 ) );
        assertThat( summary.counters().labelsRemoved(), is( 3 ) );
        assertThat( summary.counters().indexesAdded(), is( 3 ) );
        assertThat( summary.counters().indexesRemoved(), is( 3 ) );
        assertThat( summary.counters().constraintsAdded(), is( 3 ) );
        assertThat( summary.counters().constraintsRemoved(), is( 3 ) );
        assertThat( summary.counters().containsUpdates(), is( true ) );

        var codes = summary.notifications().stream().map( Notification::code ).collect( Collectors.toList() );
        assertThat( codes, containsInAnyOrder( codeOf( NotificationCode.DEPRECATED_PROCEDURE ), codeOf( NotificationCode.RUNTIME_UNSUPPORTED ) ) );
    }

    @Test
    void testExplain()
    {
        when( graph0Result.columns() ).thenReturn( null );
        when( graph0Result.summary() ).thenReturn( null );
        when( graph0Result.records() ).thenReturn( null );
        when( graph1Result.columns() ).thenReturn( null );
        when( graph1Result.summary() ).thenReturn( null );
        when( graph1Result.records() ).thenReturn( null );

        Transaction tx = transaction( "mega", AccessMode.READ );
        ResultSummary summary = tx.run( String.join( "\n",
                "EXPLAIN",
                "CALL { FROM mega.graph(0) RETURN 1 AS x }",
                "CALL { FROM mega.graph(1) CREATE () RETURN 1 AS y }",
                "RETURN y, x"
        ) ).summary();
        tx.success();

        String nl = System.lineSeparator();

        assertThat( summary.statementType(), is( StatementType.READ_WRITE ) );

        assertThat( summary.hasPlan(), is( true ) );
        Plan plan = summary.plan();
        assertThat( plan.operatorType(), is( "ChainedQuery" ) );
        assertThat( plan.identifiers(), contains( "y", "x" ) );
        assertThat( plan.children().size(), is( 3 ) );

        Plan c0 = plan.children().get( 0 );
        assertThat( c0.operatorType(), is( "Apply" ) );
        assertThat( c0.identifiers(), containsInAnyOrder( "x" ) );
        Plan c0c0 = c0.children().get( 0 );
        assertThat( c0c0.operatorType(), is( "Direct" ) );
        assertThat( c0c0.identifiers(), containsInAnyOrder( "x" ) );
        Plan c0c0c0 = c0c0.children().get( 0 );
        assertThat( c0c0c0.operatorType(), is( "RemoteQuery" ) );
        assertThat( c0c0c0.identifiers(), containsInAnyOrder( "x" ) );
        assertThat( c0c0c0.arguments().get( "query" ), is( org.neo4j.driver.Values.value( "RETURN 1 AS x" ) ) );

        Plan c1 = plan.children().get( 1 );
        assertThat( c1.operatorType(), is( "Apply" ) );
        assertThat( c1.identifiers(), containsInAnyOrder( "x", "y" ) );
        Plan c1c0 = c1.children().get( 0 );
        assertThat( c1c0.operatorType(), is( "Direct" ) );
        assertThat( c1c0.identifiers(), containsInAnyOrder( "y" ) );
        Plan c1c0c0 = c1c0.children().get( 0 );
        assertThat( c1c0c0.operatorType(), is( "RemoteQuery" ) );
        assertThat( c1c0c0.arguments().get( "query" ), is( org.neo4j.driver.Values.value( String.join( nl, "CREATE ()", "RETURN 1 AS y" ) ) ) );
        assertThat( c1c0c0.identifiers(), containsInAnyOrder( "y" ) );

        Plan c2 = plan.children().get( 2 );
        assertThat( c2.operatorType(), is( "Direct" ) );
        assertThat( c2.identifiers(), containsInAnyOrder( "x", "y" ) );
        Plan c2c0 = c2.children().get( 0 );
        assertThat( c2c0.operatorType(), is( "LocalQuery" ) );
        assertThat( c2c0.identifiers(), containsInAnyOrder( "x", "y" ) );
    }

    private String codeOf( NotificationCode notificationCode )
    {
        return notificationCode.notification( new InputPosition( 0, 0, 0 ) ).getCode();
    }

    private Flux<com.neo4j.fabric.stream.Record> recs( com.neo4j.fabric.stream.Record... records )
    {
        return Flux.just( records );
    }

    private com.neo4j.fabric.stream.Record rec( AnyValue... vals )
    {
        return Records.of( vals );
    }

    private void verifySessionConfig( int times, org.neo4j.bolt.runtime.AccessMode accessMode )
    {
        var allValues = accessModeArgument.getAllValues();

        assertEquals( times, allValues.size() );
        assertTrue( allValues.stream().allMatch( ac -> ac.equals( accessMode ) ) );
    }
}
