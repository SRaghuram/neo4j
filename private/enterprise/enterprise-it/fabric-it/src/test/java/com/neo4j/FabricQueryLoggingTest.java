/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.driver.AutoCommitStatementResult;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.test.routing.FabricEverywhereExtension;
import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.AssertableQueryExecutionMonitor.endFailure;
import static com.neo4j.AssertableQueryExecutionMonitor.endSuccess;
import static com.neo4j.AssertableQueryExecutionMonitor.query;
import static com.neo4j.AssertableQueryExecutionMonitor.startExecution;
import static com.neo4j.AssertableQueryExecutionMonitor.startProcessing;
import static com.neo4j.AssertableQueryExecutionMonitor.throwable;
import static java.nio.file.Files.readAllLines;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@TestDirectoryExtension
@ExtendWith( {FabricEverywhereExtension.class, SuppressOutputExtension.class} )
class FabricQueryLoggingTest
{
    @Inject
    TestDirectory testDirectory;

    private TestFabric testFabric;
    private Driver clientDriver;
    private final DriverPool driverPool = mock( DriverPool.class );

    private final AutoCommitStatementResult graph0Result = mock( AutoCommitStatementResult.class );
    private final AutoCommitStatementResult graph1Result = mock( AutoCommitStatementResult.class );
    private AssertableQueryExecutionMonitor.Monitor queryExecutionMonitor;

    private final DriverUtils mega = new DriverUtils( "mega" );
    private final DriverUtils neo4j = new DriverUtils( "neo4j" );

    @BeforeEach
    void beforeEach()
    {
        var additionalSettings = Map.of(
                "dbms.directories.neo4j_home", testDirectory.homePath().toString(),
                "dbms.logs.query.enabled", "VERBOSE",
                "fabric.graph.0.uri", "bolt://localhost:1111",
                "fabric.graph.1.uri", "bolt://localhost:2222"
        );

        queryExecutionMonitor = new AssertableQueryExecutionMonitor.Monitor();

        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withAdditionalSettings( additionalSettings )
                .addMocks( driverPool )
                .withQueryExecutionMonitor( queryExecutionMonitor )
                .withHomeDir( testDirectory.homeDir().toPath() )
                .build();

        clientDriver = testFabric.directClientDriver();

        mockResult( graph0Result );
        mockResult( graph1Result );

        mockDriverPool( createMockDriver( graph0Result ), createMockDriver( graph1Result ) );
    }

    @AfterEach
    void afterEach()
    {
        testFabric.close();
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
        when( tx.rollback() ).thenReturn( Mono.empty() );

        when( mockDriver.beginTransaction( any(), any(), any(), any() ) ).thenReturn( Mono.just( tx ) );
        return mockDriver;
    }

    @Test
    void testQueryLoggingSingleChildQuery()
    {
        String query = joinAsLines(
                "RETURN 1 AS x"
        );

        doInTx( neo4j, AccessMode.READ, tx -> tx.run( query ).consume() );

        assertThat( queryExecutionMonitor.events, contains(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.of( "neo4j" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                startExecution()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.of( "neo4j" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planning" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.of( "neo4j" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
    }

    @Test
    void testQueryLoggingFabricSingleQuery()
    {
        String query = joinAsLines(
                "RETURN 1 AS x"
        );

        doInTx( mega, AccessMode.READ, tx -> tx.run( query ).consume() );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
    }

    @Test
    void testQueryLoggingFabricSubquery()
    {
        String query = joinAsLines(
                "UNWIND [0, 1] AS s",
                "CALL { USE neo4j RETURN 2 AS y }",
                "RETURN s, y"
        );

        doInTx( mega, AccessMode.READ, tx -> tx.run( query ).consume() );

        assertThat( queryExecutionMonitor.events.stream()
                                                .map( e -> e.query.id() )
                                                .distinct().count()
        ).isEqualTo( 5 );

        // 1 top-level query
        assertThat( queryExecutionMonitor.events.stream()
                                                .filter( e -> e.query.databaseId().isEmpty() )
                                                .map( e -> e.query.id() )
                                                .distinct().count()
        ).isEqualTo( 1 );

        // 2 child queries against 'mega'
        assertThat( queryExecutionMonitor.events.stream()
                                                .filter( e -> e.query.databaseId().map( NamedDatabaseId::name ).equals( Optional.of( "mega" ) ) )
                                                .map( e -> e.query.id() )
                                                .distinct().count()
        ).isEqualTo( 2 );

        // 2 separate child queries against 'neo4j'
        assertThat( queryExecutionMonitor.events.stream()
                                                .filter( e -> e.query.databaseId().map( NamedDatabaseId::name ).equals( Optional.of( "neo4j" ) ) )
                                                .map( e -> e.query.id() )
                                                .distinct().count()
        ).isEqualTo( 2 );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                startExecution()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planning" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                startExecution()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planned" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "neo4j" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                startExecution()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "neo4j" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planned" ) ),
                endSuccess()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query for parent query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "neo4j" ) ) ) )
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

        assertThat( catchThrowable( () -> doInTx( mega, AccessMode.READ, tx -> tx.run( query ).consume() ) ) )
                .hasMessageContaining( "my failure!" );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                startExecution()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planning" ) ),
                endFailure()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, is( query ) )
                                .where( "dbName", this::dbName, is( Optional.empty() ) ) )
                        .where( "failure", e -> e.failureMessage, containsString( "my failure!" ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );

        assertThat( queryExecutionMonitor.events, containsInRelativeOrder(
                startProcessing()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "parsing" ) ),
                startExecution()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "planned" ) ),
                endFailure()
                        .where( "query", e -> e.query, query()
                                .where( "queryText", ExecutingQuery::rawQueryText, containsString( "Internal query" ) )
                                .where( "dbName", this::dbName, is( Optional.of( "mega" ) ) ) )
                        .where( "failure", e -> e.failure, throwable( is( FabricException.class ), containsString( "my failure!" ) ) )
                        .where( "status", e -> e.snapshot.status(), is( "running" ) )
        ) );
    }

    @Test
    void testLogOutputOfNormalQuery() throws IOException
    {
        String query = joinAsLines(
                "RETURN 1 AS x"
        );

        doInTx( neo4j, AccessMode.READ, tx -> tx.run( query ).consume() );

        Path logFile = getQueryLog();
        assertEventually( () -> testDirectory.getFileSystem().fileExists( logFile.toFile() ), TRUE, 1, MINUTES );
        List<String> logLines = readAllLines( logFile );

        assertEquals( 2, logLines.size() );
        assertThat( logLines.get( 0 ) ).containsPattern( "Query started: id:[0-9]+" );
        assertThat( logLines.get( 1 ) ).containsPattern( "id:[0-9]+" );
    }

    @Test
    void testLogOutputOfFabricQuery() throws Exception
    {
        when( graph0Result.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" ) ), rec( Values.stringValue( "b" ) ) )
        );

        //This query will turn into 1 Fabric query (parent) with 2 local queries and 1 remote query
        String query = "WITH 1 AS x CALL { USE mega.graph(0) RETURN 2 AS y } RETURN x, y";

        doInTx( mega, AccessMode.READ, tx -> tx.run( query ).consume() );

        Path logFile = getQueryLog();
        assertEventually( () -> testDirectory.getFileSystem().fileExists( logFile.toFile() ), TRUE, 1, MINUTES );
        List<String> logLines = readAllLines( logFile );
        assertEquals( 6, logLines.size() ); //3 queries logged, start + end of each

        Pattern pattern = Pattern.compile( "Query started: id:((Fabric-)?[0-9]+)" );
        Set<String> ids = logLines.stream()
                                  .map( pattern::matcher )
                                  .filter( Matcher::find )
                                  .map( m -> m.group( 1 ) )
                                  .collect( Collectors.toSet() );
        assertThat( ids, hasSize( 3 ) ); //has 3 started queries with unique ids
    }

    private Path getQueryLog()
    {
        return testDirectory.homeDir().toPath().resolve( GraphDatabaseSettings.logs_directory.defaultValue() ).resolve( "query.log" );
    }

    private Optional<String> dbName( ExecutingQuery query )
    {
        return query.databaseId().map( NamedDatabaseId::name );
    }

    private Flux<Record> recs( Record... records )
    {
        return Flux.just( records );
    }

    private Record rec( AnyValue... values )
    {
        return Records.of( values );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void doInTx( DriverUtils driverUtils, AccessMode mode, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( clientDriver, mode, workload );
    }
}
