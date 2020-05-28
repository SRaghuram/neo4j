/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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

    private final Location.Remote location = new Location.Remote.External( 0, null, null, null );
    private final FabricTransactionInfo transactionInfo = new FabricTransactionInfo( null, null, null, null, false, Duration.ZERO, null, null );

    @BeforeEach
    void setUp()
    {
        when( driver.asyncSession( any() ) ).thenReturn( session );
        when( session.beginTransactionAsync( any() ) ).thenReturn( CompletableFuture.completedFuture( asyncTransaction ) );
        when( session.lastBookmark() ).thenReturn( InternalBookmark.parse(  "BB" ) );

        when( asyncTransaction.commitAsync() ).thenReturn( CompletableFuture.completedFuture( null ) );
        when( asyncTransaction.rollbackAsync() ).thenReturn( CompletableFuture.completedFuture( null ) );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( null ) );
    }

    @Test
    void testBasicLifecycleWithCommit()
    {
        var cursor = new TestStatementResultCursor( List.of( "a", "b" ), List.of( createDriverRecord( "a1", "b1" ) ) );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( cursor ) );

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );
        var columns = statementResult.columns().collectList().block();

        assertEquals( List.of( "a", "b" ), columns );

        var records = statementResult.records().collectList().block();
        assertEquals( List.of( createFabricRecord( "a1", "b1" ) ), records );

        var bookmark = fabricTransaction.commit().block();
        assertEquals( "BB", bookmark.getSerialisedState() );

        verify( asyncTransaction ).commitAsync();
        verify( session ).closeAsync();
    }

    @Test
    void testBasicLifecycleWithRollback()
    {
        var cursor = new TestStatementResultCursor( List.of( "a", "b" ), List.of() );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( cursor ) );

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
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

    @Test
    void testClientErrorWithKnownCode()
    {
        when( asyncTransaction.runAsync( any(), anyMap() ) )
                .thenReturn( failedFuture( new ClientException( Status.Statement.SyntaxError.code().serialize(), "Something went wrong" ) ));

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );

        try
        {
            statementResult.columns().collectList().block();
            fail( "Exception expected" );
        }
        catch ( FabricException e )
        {
            assertEquals( Status.Statement.SyntaxError, e.status() );
            assertEquals( "Something went wrong", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    @Test
    void testClientErrorWithUnknownCode()
    {
        when( asyncTransaction.runAsync( any(), anyMap() ) )
                .thenReturn( failedFuture( new ClientException( "SomeCode", "Something went wrong" ) ));

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );

        try
        {
            statementResult.columns().collectList().block();
            fail( "Exception expected" );
        }
        catch ( FabricException e )
        {
            assertEquals( Status.Statement.RemoteExecutionFailed, e.status() );
            assertEquals( "Remote execution failed with code SomeCode and message 'Something went wrong'", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    @Test
    void testServerErrorWithKnownCode()
    {
        when( asyncTransaction.runAsync( any(), anyMap() ) )
                .thenReturn( failedFuture( new DatabaseException( Status.Statement.ExecutionFailed.code().serialize(), "Something went wrong" ) ));

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );

        try
        {
            statementResult.columns().collectList().block();
            fail( "Exception expected" );
        }
        catch ( FabricException e )
        {
            assertEquals( Status.Statement.RemoteExecutionFailed, e.status() );
            assertEquals( "Remote execution failed with code Neo.DatabaseError.Statement.ExecutionFailed and message 'Something went wrong'", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    @Test
    void testServerErrorWithUnknownCode()
    {
        when( asyncTransaction.runAsync( any(), anyMap() ) )
                .thenReturn( failedFuture( new DatabaseException( "SomeCode", "Something went wrong" ) ));

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
        var statementResult = fabricTransaction.run( "Some query", MapValue.EMPTY );

        try
        {
            statementResult.columns().collectList().block();
            fail( "Exception expected" );
        }
        catch ( FabricException e )
        {
            assertEquals( Status.Statement.RemoteExecutionFailed, e.status() );
            assertEquals( "Remote execution failed with code SomeCode and message 'Something went wrong'", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    private void testRecordStream( int batchSize )
    {
        var cursor = new TestStatementResultCursor( List.of( "a", "b" ), List.of(
                createDriverRecord( "a1", "b1" ),
                createDriverRecord( "a2", "b2" ),
                createDriverRecord( "a3", "b3" )
        ) );
        when( asyncTransaction.runAsync( any(), anyMap() ) ).thenReturn( CompletableFuture.completedFuture( cursor ) );

        var fabricTransaction = asyncPooledDriver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() ).block();
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

    private org.neo4j.fabric.stream.Record createFabricRecord( String... values )
    {
        var convertedValues =
                Arrays.stream( values ).map( org.neo4j.values.storable.Values::stringValue ).map( v -> (AnyValue) v ).collect( Collectors.toList() );
        return Records.of( convertedValues );
    }

    private static class TestStatementResultCursor implements ResultCursor
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
