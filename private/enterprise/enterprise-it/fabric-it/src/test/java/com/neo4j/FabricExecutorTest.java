/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.config.FabricSettings;
import com.neo4j.fabric.driver.AutoCommitStatementResult;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.driver.RemoteBookmark;
import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.PartialSummary;
import com.neo4j.utils.DriverUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.AssertableQueryExecutionMonitor.endFailure;
import static com.neo4j.AssertableQueryExecutionMonitor.endSuccess;
import static com.neo4j.AssertableQueryExecutionMonitor.query;
import static com.neo4j.AssertableQueryExecutionMonitor.start;
import static com.neo4j.AssertableQueryExecutionMonitor.throwable;
import static java.nio.file.Files.readAllLines;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
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
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestDirectoryExtension
class FabricExecutorTest
{
    @Inject
    TestDirectory testDirectory;

    private Config config;
    private TestServer testServer;
    private Driver clientDriver;
    private DriverPool driverPool = mock( DriverPool.class );

    private ArgumentCaptor<org.neo4j.bolt.runtime.AccessMode> accessModeArgument = ArgumentCaptor.forClass( org.neo4j.bolt.runtime.AccessMode.class );
    private final AutoCommitStatementResult graph0Result = mock( AutoCommitStatementResult.class );
    private final AutoCommitStatementResult graph1Result = mock( AutoCommitStatementResult.class );
    private AssertableLogProvider internalLogProvider;
    private AssertableQueryExecutionMonitor.Monitor queryExecutionMonitor;
    private DriverUtils driverUtils;

    @AfterEach
    void afterAll()
    {
        testServer.stop();
        clientDriver.closeAsync();
    }

    @BeforeEach
    void beforeEach()
    {
        config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.homeDir().toPath() )
                .set( FabricSettings.databaseName, "mega" )
                .set( FabricSettings.GraphSetting.of( "0" ).uris, List.of( URI.create( "bolt://localhost:1111" ) ) )
                .set( FabricSettings.GraphSetting.of( "1" ).uris, List.of( URI.create( "bolt://localhost:2222" ) ) )
                .set( BoltConnector.listen_address, new SocketAddress( "0.0.0.0", 0 ) )
                .set( BoltConnector.enabled, true )
                .set( GraphDatabaseSettings.log_queries, GraphDatabaseSettings.LogQueryLevel.VERBOSE )
                .build();

        testServer = new TestServer( config, config.get( GraphDatabaseSettings.neo4j_home ) );

        testServer.addMocks( driverPool );
        internalLogProvider = new AssertableLogProvider();
        testServer.setLogService( new SimpleLogService( NullLogProvider.getInstance(), internalLogProvider ) );
        testServer.start();

        queryExecutionMonitor = new AssertableQueryExecutionMonitor.Monitor();
        testServer.getDependencies().resolveDependency( Monitors.class )
                .addMonitorListener( queryExecutionMonitor );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltDirectUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );

        driverUtils = new DriverUtils( "mega" );

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
        doReturn( graph0 ).when( driverPool ).getDriver( argThat( g -> g.getGraphId() == 0 ), any() );
        doReturn( graph1 ).when( driverPool ).getDriver( argThat( g -> g.getGraphId() == 1 ), any() );
    }

    private PooledDriver createMockDriver( StatementResult mockStatementResult )
    {
        PooledDriver mockDriver = mock( PooledDriver.class );

        FabricDriverTransaction tx = mock( FabricDriverTransaction.class );

        when( tx.run( any(), any() ) ).thenReturn( mockStatementResult );
        when( tx.commit() ).thenReturn( Mono.just( new RemoteBookmark( "BB" ) ) );
        when( tx.rollback() ).thenReturn(Mono.empty());

        when( mockDriver.beginTransaction( any(), accessModeArgument.capture(), any(), any() ) ).thenReturn( Mono.just( tx ) );
        return mockDriver;
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

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
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

        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testWriteInReadSession()
    {
        var ex = assertThrows( ClientException.class, () -> doInMegaTx( AccessMode.READ, tx ->
                tx.run( "USE mega.graph(0) CREATE (n:Foo)" ).consume() )
        );

        assertThat( ex.getMessage(), containsString( "Writing in read access mode not allowed" ) );
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
        doInMegaSession( AccessMode.READ, session ->
        {
            var tx = session.beginTransaction();

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

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testMixedInReadSessionGraph1()
    {
        doInMegaSession( AccessMode.READ, session ->
        {
            var tx = session.beginTransaction();

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

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testMixedInWriteSession()
    {
        doInMegaSession( AccessMode.WRITE, session ->
        {
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

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.WRITE );
        verifySessionConfig( 0, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testLocalReturnQuery()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            List<Record> list = tx.run( joinAsLines( "RETURN 1 AS x" ) ).list();

            assertEquals( 1, list.get( 0 ).get( "x" ).asLong() );
        } );
    }

    @Test
    void testLocalFlatCompositeQuery()
    {
        doInMegaTx( AccessMode.READ, tx ->
        {
            List<Record> list = tx.run( joinAsLines(
                    "UNWIND [1, 2] AS x",
                    "CALL { RETURN 'y' AS y }",
                    "CALL { UNWIND [8, 9] AS z RETURN z }",
                    "RETURN x, y, z ORDER BY x, y, z"
            ) ).list();

            assertEquals(  1, list.get( 0 ).get( "x" ).asLong() );
            assertEquals(  "y", list.get( 0 ).get( "y" ).asString() );
            assertEquals(  8, list.get( 0 ).get( "z" ).asLong() );

            assertEquals(  1, list.get( 1 ).get( "x" ).asLong() );
            assertEquals(  "y", list.get( 1 ).get( "y" ).asString() );
            assertEquals(  9, list.get( 1 ).get( "z" ).asLong() );

            assertEquals(  2, list.get( 2 ).get( "x" ).asLong() );
            assertEquals(  "y", list.get( 2 ).get( "y" ).asString() );
            assertEquals(  8, list.get( 2 ).get( "z" ).asLong() );

            assertEquals(  2, list.get( 3 ).get( "x" ).asLong() );
            assertEquals(  "y", list.get( 3 ).get( "y" ).asString() );
            assertEquals(  9, list.get( 3 ).get( "z" ).asLong() );
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

            assertEquals( 0, list.get( 0 ).get( "s" ).asLong() );
            assertEquals( "a", list.get( 0 ).get( "y" ).asString()  );

            assertEquals( 0, list.get( 1 ).get( "s" ).asLong() );
            assertEquals( "b", list.get( 1 ).get( "y" ).asString()  );

            assertEquals( 1, list.get( 2 ).get( "s" ).asLong() );
            assertEquals( "k", list.get( 2 ).get( "y" ).asString()  );

            assertEquals( 1, list.get( 3 ).get( "s" ).asLong() );
            assertEquals( "l", list.get( 3 ).get( "y" ).asString()  );
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

            assertThat( summary.queryType(), is( QueryType.READ_WRITE ) );

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

            assertThat( summary.queryType(), is( QueryType.READ_WRITE ) );

            assertThat( summary.hasPlan(), is( true ) );
            Plan plan = summary.plan();
            assertThat( plan.operatorType(), is( "Leaf" ) );
            assertThat( plan.identifiers(), contains( "y", "x" ) );
            assertThat( plan.children().size(), is( 1 ) );

            Plan c0 = plan.children().get( 0 );
            assertThat( c0.operatorType(), is( "Apply" ) );
            assertThat( c0.identifiers(), contains( "x", "y" ) );
            Plan c0c1 = c0.children().get( 1 );
            assertThat( c0c1.operatorType(), is( "Leaf" ) );
            assertThat( c0c1.identifiers(), contains( "y" ) );
            assertThat( c0c1.arguments().get( "query" ),
                        is( org.neo4j.driver.Values.value( joinAsLines( "USE `mega`.`graph`((1))", "CREATE ()", "RETURN 1 AS `y`" ) ) ) );

            Plan c0c0 = c0.children().get( 0 );
            assertThat( c0c0.operatorType(), is( "Apply" ) );
            assertThat( c0c0.identifiers(), contains( "x" ) );
            Plan c0c0c1 = c0c0.children().get( 1 );
            assertThat( c0c0c1.operatorType(), is( "Leaf" ) );
            assertThat( c0c0c1.identifiers(), contains( "x" ) );
            assertThat( c0c0c1.arguments().get( "query" ),
                        is( org.neo4j.driver.Values.value( joinAsLines( "USE `mega`.`graph`((0))", "RETURN 1 AS `x`" ) ) ) );

            Plan c0c0c0 = c0c0.children().get( 0 );
            assertThat( c0c0c0.operatorType(), is( "Init" ) );
            assertThat( c0c0c0.identifiers().size(), is( 0 ) );

        } );
    }

    @Test
    void testPlanLogging()
    {
        doInMegaTx( AccessMode.READ, tx -> tx.run( joinAsLines(
                "CYPHER debug=fabriclogplan",
                "RETURN 1"
        ) ).consume() );

        assertThat( internalLogProvider ).forClass( FabricExecutor.class )
                .forLevel( DEBUG ).containsMessages( "Fabric plan:" );
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

        assertThat( internalLogProvider ).forClass( FabricExecutor.class ).forLevel( DEBUG )
                .containsMessages( "local 2: /* FullyParsedQuery */ UNWIND [0, 1] AS s RETURN s AS s",
                                   "remote 0: RETURN 2 AS `y`",
                                   "remote 1: RETURN 2 AS `y`",
                                   "local 2: /* FullyParsedQuery */ InputDataStream(Vector(Variable(s), Variable(y))) " +
                                           "RETURN s AS s, y AS y ORDER BY s ASCENDING, y ASCENDING" );
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
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
    }

    @Test
    void testFullQueryLogging() throws Exception
    {
        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" ) ), rec( Values.stringValue( "b" ) ) )
        );

        when( graph1Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "k" ) ), rec( Values.stringValue( "l" ) ) )
        );

        //This query will turn into 1 Fabric query (parent) with 2 child queries
        String query = "UNWIND [0, 1] AS s CALL { USE mega.graph(s) RETURN 2 AS y } RETURN s, y ORDER BY s, y";

        doInMegaTx( AccessMode.READ, tx -> tx.run( query ).consume() );

        Path logFile = config.get( GraphDatabaseSettings.logs_directory ).resolve( "query.log" );
        assertEventually( () -> testDirectory.getFileSystem().fileExists( logFile.toFile() ), TRUE, 1, MINUTES );
        List<String> logLines = readAllLines( logFile );
        assertEquals( 6, logLines.size() ); //3 queries logged, start + end of each

        Pattern compile = Pattern.compile( "Query started: id:((Fabric-)?[0-9]+)" );
        Set<String> ids = logLines.stream()
                .map( compile::matcher )
                .filter( Matcher::find )
                .map( m -> m.group( 1 ) )
                .collect( Collectors.toSet() );
        assertThat( ids, hasSize( 3 ) ); //has 3 started queries with unique ids
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
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", q -> q.databaseId().name(), is( "mega" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) ),
                endFailure()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
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
        driverUtils.doInTx( clientDriver, mode, workload );
    }

    private void doInMegaSession( AccessMode mode, Consumer<Session> workload )
    {
        driverUtils.doInSession( clientDriver, mode, workload );
    }
}
