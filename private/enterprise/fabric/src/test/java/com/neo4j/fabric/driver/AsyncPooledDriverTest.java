/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncPooledDriverTest
{

    private final Driver driver = mock( Driver.class );
    private final Consumer<PooledDriver> releaseCallback = mock( Consumer.class );
    private final AsyncPooledDriver asyncPooledDriver = new AsyncPooledDriver( driver, releaseCallback );
    private final AsyncSession session = mock( AsyncSession.class );
    private final AsyncTransaction asyncTransaction = mock( AsyncTransaction.class );

    private final FabricConfig.Graph location = new FabricConfig.Graph( 1, URI.create( "neo4j://somewhere:1234" ), "db1", null, null );
    private final FabricTransactionInfo transactionInfo = new FabricTransactionInfo( null, null, null, null, false, Duration.ZERO, null );

    @BeforeEach
    void setUp()
    {
        when( driver.asyncSession( any() ) ).thenReturn( session );
        when( session.beginTransactionAsync() ).thenReturn( CompletableFuture.completedFuture( asyncTransaction ) );

        when( asyncTransaction.commitAsync() ).thenReturn( CompletableFuture.completedFuture( null ) );
        when( asyncTransaction.rollbackAsync() ).thenReturn( CompletableFuture.completedFuture( null ) );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( null ) );
    }

    @Test
    void testBasicLifecycleWithCommit()
    {
        var cursor = new TestStatementResultCursor( List.of( "a", "b" ), List.of( createDriverRecord( "a1", "b1" ) ) );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( cursor ) );

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );
        var columns = statementResult.columns().collectList().block();

        assertEquals( List.of( "a", "b" ), columns );

        var records = statementResult.records().collectList().block();
        assertEquals( List.of( createFabricRecord( "a1", "b1" ) ), records );

        fabricTransaction.commit().block();

        verify( asyncTransaction ).commitAsync();
        verify( session ).closeAsync();
    }

    @Test
    void testBasicLifecycleWithRollback()
    {
        var cursor = new TestStatementResultCursor( List.of( "a", "b" ), List.of() );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( cursor ) );

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );
        var columns = statementResult.columns().collectList().block();

        assertEquals( List.of( "a", "b" ), columns );

        var records = statementResult.records().collectList().block();
        assertTrue( records.isEmpty() );

        fabricTransaction.rollback().block();

        verify( asyncTransaction ).rollbackAsync();
        verify( session ).closeAsync();
    }

    @Test
    void testRequestOneByOne()
    {
        testRecordStream( 1 );
    }

    @Test
    void testRequestInBatches()
    {
        testRecordStream( 2 );
    }

    private void testRecordStream( int batchSize )
    {
        var cursor = new TestStatementResultCursor( List.of( "a", "b" ), List.of(
                createDriverRecord( "a1", "b1" ),
                createDriverRecord( "a2", "b2" ),
                createDriverRecord( "a3", "b3" )
        ) );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( cursor ) );

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );

        var records = statementResult.records().limitRate( batchSize ).collectList().block();
        assertEquals( List.of( createFabricRecord( "a1", "b1" ), createFabricRecord( "a2", "b2" ), createFabricRecord( "a3", "b3" ) ), records );
    }

    private CompletionStage<Record> createDriverRecord( String... values )
    {
        var convertedValues = Arrays.stream( values ).map( Values::value ).collect( Collectors.toList() );

        Record record = mock( Record.class );
        when( record.size() ).thenReturn( values.length );
        when( record.values() ).thenReturn( convertedValues );

        return CompletableFuture.completedFuture( record );
    }

    private com.neo4j.fabric.stream.Record createFabricRecord( String... values )
    {
        var convertedValues =
                Arrays.stream( values ).map( org.neo4j.values.storable.Values::stringValue ).map( v -> (AnyValue) v ).collect( Collectors.toList() );
        return Records.of( convertedValues );
    }

    private static class TestStatementResultCursor implements StatementResultCursor
    {

        private final Queue<CompletionStage<Record>> records = new ArrayDeque<>();
        private final List<String> keys;

        TestStatementResultCursor( List<String> keys, List<CompletionStage<Record>> records )
        {
            this.keys = keys;
            records.forEach( this.records::offer );
            this.records.offer( CompletableFuture.completedFuture( null ) );
        }

        @Override
        public List<String> keys()
        {
            return keys;
        }

        @Override
        public CompletionStage<Record> nextAsync()
        {
            return records.poll();
        }

        @Override
        public CompletionStage<Record> peekAsync()
        {
            return records.peek();
        }

        private <T> T notImplemented()
        {
            throw new IllegalStateException( "Not implemented" );
        }

        @Override
        public CompletionStage<ResultSummary> summaryAsync()
        {
            return notImplemented();
        }

        @Override
        public CompletionStage<Record> singleAsync()
        {
            return notImplemented();
        }

        @Override
        public CompletionStage<ResultSummary> consumeAsync()
        {
            return notImplemented();
        }

        @Override
        public CompletionStage<ResultSummary> forEachAsync( Consumer<Record> action )
        {
            return notImplemented();
        }

        @Override
        public CompletionStage<List<Record>> listAsync()
        {
            return notImplemented();
        }

        @Override
        public <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
        {
            return notImplemented();
        }
    }
}
