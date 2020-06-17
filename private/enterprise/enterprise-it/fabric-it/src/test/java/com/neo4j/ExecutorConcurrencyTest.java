/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.driver.Driver;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.EmptySummary;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class ExecutorConcurrencyTest
{
    private static Driver clientDriver;
    private static TestFabric testFabric;

    private static FabricRemoteExecutor remoteExecutor = mock( FabricRemoteExecutor.class );
    private static DriverUtils driverUtils;

    private final List<RemoteQueryRecord> remoteQueryRecords = mockRemoteQueries( 10 );
    private final FabricRemoteExecutor.RemoteTransactionContext fabricRemoteTransactionContext = mock( FabricRemoteExecutor.RemoteTransactionContext.class );

    @BeforeAll
    static void beforeAll()
    {

        Map<String,String> additionalConfigProperties = new HashMap<>();
        additionalConfigProperties.put( "fabric.graph.0.uri", "bolt://mega:1000" );
        additionalConfigProperties.put( "fabric.graph.1.uri", "bolt://mega:1001" );
        additionalConfigProperties.put( "fabric.graph.2.uri", "bolt://mega:1002" );
        additionalConfigProperties.put( "fabric.graph.3.uri", "bolt://mega:1003" );
        additionalConfigProperties.put( "fabric.graph.4.uri", "bolt://mega:1004" );
        additionalConfigProperties.put( "fabric.stream.concurrency", "3" );
        additionalConfigProperties.put( "fabric.stream.batch_size", "6" );
        additionalConfigProperties.put( "fabric.stream.buffer.size", "6" );
        additionalConfigProperties.put( "fabric.stream.buffer.low_watermark", "0" );

        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withAdditionalSettings( additionalConfigProperties )
                .addMocks( remoteExecutor )
                .build();

        clientDriver = testFabric.routingClientDriver();
        driverUtils = new DriverUtils( "mega" );
    }

    @BeforeEach
    void beforeEach()
    {
        mockRemoteExecutor();
    }

    private FabricRemoteExecutor mockRemoteExecutor()
    {
        when( remoteExecutor.startTransactionContext( any(), any(), any() ) ).thenReturn( fabricRemoteTransactionContext );

        var counter = new AtomicInteger( 0 );

        when( fabricRemoteTransactionContext.run( any(), any(), any(), any() ) ).thenAnswer( invocationOnMock ->
        {
            int queryCount = counter.getAndIncrement();
            return Mono.just( remoteQueryRecords.get( queryCount ).statementResult );
        } );

        return remoteExecutor;
    }

    private static List<RemoteQueryRecord> mockRemoteQueries( int txCount )
    {
        return IntStream.range( 0, txCount ).mapToObj( RemoteQueryRecord::new ).collect( Collectors.toList() );
    }

    @AfterAll
    static void afterAll()
    {
        testFabric.close();
    }

    @Test
    void testParallelism()
    {
        var records = driverUtils.inRxTx( clientDriver, tx -> {
            var query = joinAsLines(
                    "UNWIND range(0, 4) AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (a)",
                    "  RETURN a",
                    "}",
                    "RETURN a" );

            var statementResult = tx.run( query );
            return Flux.from( statementResult.records() ).limitRequest( 10 ).take( 10 ).collectList().block();
        });

        // Prefetch has quite complex logic, so this test only checks
        // that at least 2 ( the configured prefetch buffer size /  the configured concurrency level )
        // records were requested from 3 ( the configured concurrency level ) remote streams
        // and the remaining 2 stream were not touched.
        assertTrue( remoteQueryRecords.get( 0 ).requested.get() >= 2 );
        assertTrue( remoteQueryRecords.get( 1 ).requested.get() >= 2 );
        assertTrue( remoteQueryRecords.get( 2 ).requested.get() >= 2 );
        assertEquals( 0, remoteQueryRecords.get( 3 ).requested.get() );
        assertEquals( 0, remoteQueryRecords.get( 4 ).requested.get() );
    }

    @Test
    void testAllStreamsFullyConsumed()
    {
        var records = driverUtils.inTx( clientDriver, tx -> {
            var query = joinAsLines(
                    "UNWIND range(0, 4) AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (a)",
                    "  RETURN a",
                    "}",
                    "RETURN a" );

            return tx.run( query ).list();
        } );

        // there are 5 streams with 20 records in each of them
        assertEquals( 100, records.size() );
    }

    private static class RemoteQueryRecord
    {
        private final AtomicLong requested = new AtomicLong( 0 );
        private final StatementResult statementResult = mock( StatementResult.class );

        private RemoteQueryRecord( int queryId )
        {
            Stream<Record> records = IntStream.range( 0, 20 ).mapToObj( j -> Records.of( List.of( Values.stringValue( queryId + "-" + j ) ) ) );
            when( statementResult.columns() ).thenReturn( Flux.just( "a" ) );
            when( statementResult.summary() ).thenReturn( Mono.just( new EmptySummary() ) );
            when( statementResult.records() ).thenReturn( Flux.fromStream( records ).doOnRequest( requested::addAndGet ) );
        }
    }
}
