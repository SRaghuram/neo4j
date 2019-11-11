/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.driver.AutoCommitStatementResult;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.PartialSummary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.AssertableQueryExecutionMonitor.endFailure;
import static com.neo4j.AssertableQueryExecutionMonitor.endSuccess;
import static com.neo4j.AssertableQueryExecutionMonitor.query;
import static com.neo4j.AssertableQueryExecutionMonitor.start;
import static com.neo4j.AssertableQueryExecutionMonitor.throwable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class FabricExecutorTest
{

    private static final PortUtils.Ports ports = PortUtils.findFreePorts();

    private static final Map<String,String> configProperties = Map.of(
            "fabric.database.name", "mega",
            "fabric.graph.0.uri", "bolt://localhost:1111",
            "fabric.graph.1.uri", "bolt://localhost:2222",
            "fabric.routing.servers", "localhost:" + ports.bolt,
            "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
            "dbms.connector.bolt.enabled", "true",
            "dbms.logs.query.enabled", "true"
    );

    private static final Config config = Config.newBuilder()
            .setRaw( configProperties )
            .build();

    private static TestServer testServer;
    private static Driver clientDriver;
    private static DriverPool driverPool = mock( DriverPool.class );

    private ArgumentCaptor<org.neo4j.bolt.runtime.AccessMode> accessModeArgument = ArgumentCaptor.forClass( org.neo4j.bolt.runtime.AccessMode.class );
    private static final AutoCommitStatementResult graph0Result = mock( AutoCommitStatementResult.class );
    private static final AutoCommitStatementResult graph1Result = mock( AutoCommitStatementResult.class );
    private static AssertableLogProvider internalLogProvider;
    private static AssertableQueryExecutionMonitor.Monitor queryExecutionMonitor;

    @BeforeAll
    static void setUp()
    {
        testServer = new TestServer( config );

        testServer.addMocks( driverPool );
        internalLogProvider = new AssertableLogProvider();
        testServer.setLogService( new SimpleLogService( NullLogProvider.getInstance(), internalLogProvider ) );
        testServer.start();

        queryExecutionMonitor = new AssertableQueryExecutionMonitor.Monitor();
        testServer.getDependencies().resolveDependency( Monitors.class )
                .addMonitorListener( queryExecutionMonitor );

        clientDriver = GraphDatabase.driver(
                "bolt://localhost:" + ports.bolt,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    @AfterAll
    static void afterAll()
    {
        testServer.stop();
        clientDriver.closeAsync();
    }

    @BeforeEach
    void beforeEach()
    {
        mockResult( graph0Result );
        mockResult( graph1Result );

        internalLogProvider.clear();

        mockDriverPool( createMockDriver( graph0Result ), createMockDriver( graph1Result ) );
    }

    private static void mockResult( StatementResult statementResult )
    {
        reset( statementResult );
        when( statementResult.columns() ).thenReturn( Flux.fromIterable( List.of( "a", "b" ) ) );
        when( statementResult.records() ).thenReturn( Flux.empty() );
        when( statementResult.summary() ).thenReturn( Mono.empty() );
    }

    private void mockDriverPool( PooledDriver graph0, PooledDriver graph1 )
    {
        reset( driverPool );
        doReturn( graph0 ).when( driverPool ).getDriver( argThat( g -> g.getId() == 0 ), any() );
        doReturn( graph1 ).when( driverPool ).getDriver( argThat( g -> g.getId() == 1 ), any() );
    }

    private PooledDriver createMockDriver( AutoCommitStatementResult mockStatementResult )
    {
        PooledDriver mockDriver = mock( PooledDriver.class );

        FabricDriverTransaction tx = mock( FabricDriverTransaction.class );

        when( tx.run( any(), any() ) ).thenReturn( mockStatementResult );
        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        when( mockDriver.beginTransaction( any(), accessModeArgument.capture(), any(), any() ) ).thenReturn( Mono.just( tx ) );

        when( mockDriver.run( any(), any(), any(), accessModeArgument.capture(), any(), any() ) ).thenReturn( mockStatementResult );
        return mockDriver;
    }

    private Transaction transaction( String database, AccessMode mode )
    {
        return clientDriver.session( SessionConfig.builder().withDatabase( database ).withDefaultAccessMode( mode ).build() ).beginTransaction();
    }

    @Test
    void testReadInReadSession()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
        } );

        verifySessionConfig( 4, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testReadInWriteSession()
    {
        doInMegaTx( AccessMode.WRITE, tx ->
        {
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
        } );

        verifySessionConfig( 4, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testWriteInReadSession()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            assertThrows( ClientException.class, () ->
                    tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume() );
        } );

        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testWriteInWriteSession()
    {
        doInMegaSession( AccessMode.WRITE, session ->
        {
            var tx = session.beginTransaction();

            tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume();
            tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume();

            verifySessionConfig( 1, org.neo4j.bolt.runtime.AccessMode.WRITE );
            verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.READ );

            assertThrows( ClientException.class, () ->
                    tx.run( "USE mega.graph(1) CREATE (n:Foo)" ).consume()
            );
        } );
    }

    @Test
    void testMixedInReadSessionGraph0()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();

            assertThrows( ClientException.class, () ->
                    tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume()
            );
        } );

        verifySessionConfig( 8, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testMixedInReadSessionGraph1()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();

            assertThrows( ClientException.class, () ->
                    tx.run( "USE mega.graph(1) CREATE (n:Foo)" ).consume()
            );
        } );

        verifySessionConfig( 8, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testMixedInWriteSession()
    {
        doInMegaSession( AccessMode.WRITE, session -> {
            var tx = session.beginTransaction();

            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume();
            tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();
            tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).consume();

            assertThrows( ClientException.class, () ->
                    tx.run( "USE mega.graph(1) CREATE (n:Foo)" ).consume()
            );
        } );

        verifySessionConfig( 1, org.neo4j.bolt.runtime.AccessMode.WRITE );
        verifySessionConfig( 6, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testLocalReturnQuery()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            List<Record> list = tx.run( joinAsLines( "RETURN 1 AS x" ) ).list();

            assertEquals( list.get( 0 ).get( "x" ).asLong(), 1 );
        } );
    }

    @Test
    void testLocalFlatCompositeQuery()
    {
        doInMegaTx(  AccessMode.READ, tx -> {
            List<Record> list = tx.run( joinAsLines(
                    "UNWIND [1, 2] AS x",
                    "CALL { RETURN 'y' AS y }",
                    "CALL { UNWIND [8, 9] AS z RETURN z }",
                    "RETURN x, y, z ORDER BY x, y, z"
            ) ).list();

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
        } );
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

        doInMegaTx( AccessMode.READ, tx ->
        {
            List<Record> list = tx.run( joinAsLines(
                    "UNWIND [0, 1] AS s",
                    "CALL { USE mega.graph(s) RETURN '' AS y }",
                    "RETURN s, y ORDER BY s, y"
            ) ).list();

            assertEquals( list.get( 0 ).get( "s" ).asLong(), 0 );
            assertEquals( list.get( 0 ).get( "y" ).asString(), "a" );

            assertEquals( list.get( 1 ).get( "s" ).asLong(), 0 );
            assertEquals( list.get( 1 ).get( "y" ).asString(), "b" );

            assertEquals( list.get( 2 ).get( "s" ).asLong(), 1 );
            assertEquals( list.get( 2 ).get( "y" ).asString(), "k" );

            assertEquals( list.get( 3 ).get( "s" ).asLong(), 1 );
            assertEquals( list.get( 3 ).get( "y" ).asString(), "l" );
        } );
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

        doInMegaTx( AccessMode.WRITE, tx ->
        {
            ResultSummary summary =
                    tx.run( joinAsLines(
                            "CALL { USE mega.graph(0) RETURN 1 AS x }",
                            "CALL { USE mega.graph(1) CREATE () RETURN 1 AS y }",
                            "RETURN 1"
                    ) ).consume();

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
        } );
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

        doInMegaTx( AccessMode.READ, tx ->
        {
            ResultSummary summary =
                    tx.run( joinAsLines(
                            "EXPLAIN",
                            "CALL { USE mega.graph(0) RETURN 1 AS x }",
                            "CALL { USE mega.graph(1) CREATE () RETURN 1 AS y }",
                            "RETURN y, x"
                    ) ).consume();

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
            assertThat( c0c0c0.arguments().get( "query" ), is( org.neo4j.driver.Values.value( "RETURN 1 AS `x`" ) ) );

            Plan c1 = plan.children().get( 1 );
            assertThat( c1.operatorType(), is( "Apply" ) );
            assertThat( c1.identifiers(), containsInAnyOrder( "x", "y" ) );
            Plan c1c0 = c1.children().get( 0 );
            assertThat( c1c0.operatorType(), is( "Direct" ) );
            assertThat( c1c0.identifiers(), containsInAnyOrder( "y" ) );
            Plan c1c0c0 = c1c0.children().get( 0 );
            assertThat( c1c0c0.operatorType(), is( "RemoteQuery" ) );
            assertThat( c1c0c0.arguments().get( "query" ), is( org.neo4j.driver.Values.value( joinAsLines( "CREATE ()", "RETURN 1 AS `y`" ) ) ) );
            assertThat( c1c0c0.identifiers(), containsInAnyOrder( "y" ) );

            Plan c2 = plan.children().get( 2 );
            assertThat( c2.operatorType(), is( "Direct" ) );
            assertThat( c2.identifiers(), containsInAnyOrder( "x", "y" ) );
            Plan c2c0 = c2.children().get( 0 );
            assertThat( c2c0.operatorType(), is( "LocalQuery" ) );
            assertThat( c2c0.identifiers(), containsInAnyOrder( "x", "y" ) );
        } );
    }

    @Test
    void testPlanLogging()
    {
        doInMegaTx( AccessMode.READ, tx -> tx.run( joinAsLines(
                "CYPHER debug=fabriclogplan",
                "RETURN 1"
        ) ).consume() );

        internalLogProvider.assertAtLeastOnce(
                inLog( FabricExecutor.class ).debug( containsString( "Fabric plan:" ) )
        );
    }

    @Test
    void testExecutionLogging()
    {
        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" ) ), rec( Values.stringValue( "b" ) ) )
        );

        when( graph1Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "k" ) ), rec( Values.stringValue( "l" ) ) )
        );

        doInMegaTx( AccessMode.READ, tx -> tx.run( joinAsLines(
                "CYPHER debug=fabriclogrecords",
                "UNWIND [0, 1] AS s",
                "CALL { USE mega.graph(s) RETURN 2 AS y }",
                "RETURN s, y ORDER BY s, y"
        ) ).consume() );

        internalLogProvider.assertAtLeastOnce(
                inLog( FabricExecutor.class ).debug( allOf( containsString( "local" ), containsString( "UNWIND [0, 1] AS s" ) ) )
        );
        internalLogProvider.assertAtLeastOnce(
                inLog( FabricExecutor.class ).debug( allOf( containsString( "remote 0" ), containsString( "RETURN 2 AS `y`" ) ) )
        );
        internalLogProvider.assertAtLeastOnce(
                inLog( FabricExecutor.class ).debug( allOf( containsString( "remote 1" ), containsString( "RETURN 2 AS `y`" ) ) )
        );
        internalLogProvider.assertAtLeastOnce(
                inLog( FabricExecutor.class ).debug(
                        allOf( containsString( "local" ), containsString( "RETURN s AS s, y AS y ORDER BY s ASCENDING, y ASCENDING" ) ) )
        );
    }

    @Test
    void testQueryLogging()
    {
        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" ) ), rec( Values.stringValue( "b" ) ) )
        );

        when( graph1Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "k" ) ), rec( Values.stringValue( "l" ) ) )
        );

        String query = joinAsLines(
                "UNWIND [0, 1] AS s",
                "CALL { USE mega.graph(s) RETURN 2 AS y }",
                "RETURN s, y ORDER BY s, y"
        );

        doInMegaTx( AccessMode.READ, tx -> tx.run( query ).consume() );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                start()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::queryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planning" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::queryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
    }

    @Test
    void testQueryLoggingFailure()
    {
        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" ) ), rec( Values.stringValue( "b" ) ) )
        );

        when( graph1Result.records() ).thenReturn(
                Flux.error( new Exception( "my failure!" ) )
        );

        String query = joinAsLines(
                "UNWIND [0, 1] AS s",
                "CALL { USE mega.graph(s) RETURN 2 AS y }",
                "RETURN s, y ORDER BY s, y"
        );
        assertThrows( Throwable.class, () -> doInMegaTx( AccessMode.READ, tx -> tx.run( query ).consume() ) );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                start()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::queryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planning" ) ),
                endFailure()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::queryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "failure", e -> e.failure, throwable( is( FabricException.class ), containsString( "my failure!" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
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
        assertEquals( times, allValues.stream().filter( ac -> ac.equals( accessMode ) ).count() );
    }

    private void doInMegaTx( AccessMode mode, Consumer<Transaction> workload )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).withDefaultAccessMode( mode ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                workload.accept( tx );
                tx.commit();
            }
        }
    }

    private void doInMegaSession( AccessMode mode, Consumer<Session> workload )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).withDefaultAccessMode( mode ).build() ) )
        {
            workload.accept( session );
        }
    }
}
