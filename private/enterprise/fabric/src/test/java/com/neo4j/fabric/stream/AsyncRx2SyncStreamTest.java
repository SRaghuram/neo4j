/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */

package com.neo4j.fabric.stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AsyncRx2SyncStreamTest
{
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    void testPositive() throws InterruptedException
    {
        RecordPublisher publisher = new RecordPublisher();
        StatementResult statementResult = mockStatementResult( publisher, List.of( "a", "b", "c" ) );
        Rx2SyncStream stream = new Rx2SyncStream( statementResult, 2, 100, 1 );

        Reader reader = new Reader( stream, 3 );

        reader.rowLatch = new CountDownLatch( 2 );
        produce( publisher, "1st batch", 2, 3 );

        executorService.submit( reader );

        assertTrue( reader.rowLatch.await( 1, TimeUnit.SECONDS ) );

        reader.rowLatch = new CountDownLatch( 7 );
        produce( publisher, "2nd batch", 7, 3 );

        assertTrue( reader.rowLatch.await( 1, TimeUnit.SECONDS ) );

        reader.rowLatch = new CountDownLatch( 2 );
        produce( publisher, "3rd batch", 2, 3 );
        publisher.close();

        assertTrue( reader.rowLatch.await( 1, TimeUnit.SECONDS ) );
        assertTrue( reader.endLatch.await( 1, TimeUnit.SECONDS ) );

        assertEquals( 11, reader.data.size() );

        verifyRecord( reader, 0, 0, "1st batch-0-0" );
        verifyRecord( reader, 0, 2, "1st batch-0-2" );
        verifyRecord( reader, 10, 0, "3rd batch-1-0" );
        verifyRecord( reader, 10, 2, "3rd batch-1-2" );
    }

    @Test
    void testError() throws InterruptedException
    {
        RecordPublisher publisher = new RecordPublisher();
        StatementResult statementResult = mockStatementResult( publisher, List.of( "a", "b", "c" ) );
        Rx2SyncStream stream = new Rx2SyncStream( statementResult, 2, 100, 1 );

        Reader reader = new Reader( stream, 3 );

        reader.rowLatch = new CountDownLatch( 2 );
        produce( publisher, "1st batch", 2, 2 );

        executorService.submit( reader );

        assertTrue( reader.rowLatch.await( 1, TimeUnit.SECONDS ) );

        reader.rowLatch = new CountDownLatch( 7 );
        produce( publisher, "2nd batch", 7, 2 );
        publisher.publicError( new RuntimeException( "Test exception" ) );

        assertTrue( reader.rowLatch.await( 1, TimeUnit.SECONDS ) );
        assertTrue( reader.endLatch.await( 1, TimeUnit.SECONDS ) );

        assertEquals( 9, reader.data.size() );

        verifyRecord( reader, 8, 0, "2nd batch-6-0" );
        verifyRecord( reader, 8, 1, "2nd batch-6-1" );
    }

    private void produce( RecordPublisher publisher, String valuePrefix, int rowCount, int columnCount )
    {
        IntStream.range( 0, rowCount ).forEach( i ->
        {
            Record record = mock( Record.class );
            IntStream.range( 0, columnCount ).forEach( j -> when( record.getValue( j ) ).thenReturn( Values.stringValue( valuePrefix + "-" + i + "-" + j ) ) );

            publisher.publish( record );
        } );
    }

    private void verifyRecord( Reader reader, int rowIdx, int columnIdx, String expectedValue )
    {
        AnyValue value = reader.data.get( rowIdx ).get( columnIdx );
        assertEquals( org.neo4j.values.storable.Values.stringValue( expectedValue ), value );
    }

    private StatementResult mockStatementResult( RecordPublisher publisher, List<String> columns )
    {
        StatementResult statementResult = mock( StatementResult.class );
        when( statementResult.columns() ).thenReturn( Flux.fromIterable( columns ) );
        when( statementResult.records() ).thenReturn( Flux.from( publisher ) );

        return statementResult;
    }

    private static class RecordPublisher implements org.reactivestreams.Publisher<Record>
    {
        private static final Record END = mock( Record.class );
        private static final Record ERROR = mock( Record.class );

        private final Queue<Record> bufferedRecords = new ArrayDeque<>();
        private final AtomicLong requested = new AtomicLong();
        private Subscriber<? super Record> subscriber;
        private RuntimeException error;

        @Override
        public void subscribe( Subscriber<? super Record> subscriber )
        {
            this.subscriber = subscriber;
            subscriber.onSubscribe( new Subscription()
            {
                @Override
                public void request( long l )
                {
                    requested.getAndAdd( l );
                    doPublish();
                }

                @Override
                public void cancel()
                {

                }
            } );
        }

        void publish( Record record )
        {
            bufferedRecords.add( record );
            doPublish();
        }

        void publicError( RuntimeException e )
        {
            error = e;
            publish( ERROR );
        }

        void close()
        {
            publish( END );
        }

        private synchronized void doPublish()
        {
            if ( subscriber == null )
            {
                return;
            }

            while ( requested.get() > 0 )
            {
                Record record = bufferedRecords.poll();

                if ( record == null )
                {
                    return;
                }

                if ( record == END )
                {
                    subscriber.onComplete();
                    return;
                }

                if ( record == ERROR )
                {
                    subscriber.onError( error );
                    return;
                }

                subscriber.onNext( record );
            }
        }
    }

    private static class Reader implements Runnable
    {

        private final Rx2SyncStream rx2SyncStream;
        private final List<List<AnyValue>> data = new ArrayList<>();
        private final CountDownLatch endLatch = new CountDownLatch( 1 );
        private final int columnCount;
        private volatile CountDownLatch rowLatch = new CountDownLatch( 1 );

        Reader( Rx2SyncStream rx2SyncStream, int columnCount )
        {
            this.rx2SyncStream = rx2SyncStream;
            this.columnCount = columnCount;
        }

        @Override
        public void run()
        {
            try
            {
                while ( true )
                {
                    Record record = rx2SyncStream.readRecord();

                    if ( record == null )
                    {
                        endLatch.countDown();
                        return;
                    }

                    List<AnyValue> row = new ArrayList<>();
                    data.add( row );

                    for ( int i = 0; i < columnCount; i++ )
                    {
                        AnyValue value = record.getValue( i );
                        row.add( value );
                    }

                    rowLatch.countDown();
                }
            }
            catch ( Exception e )
            {
                endLatch.countDown();
            }
        }
    }
}
