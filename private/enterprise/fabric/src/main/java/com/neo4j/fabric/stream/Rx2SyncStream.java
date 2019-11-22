/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import com.neo4j.fabric.executor.Exceptions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.api.exceptions.Status;

public class Rx2SyncStream
{
    private static final RecordOrError END = new RecordOrError( null, null );
    private final RecordSubscriber recordSubscriber;
    private final BlockingQueue<RecordOrError> buffer;
    private final int batchSize;

    public Rx2SyncStream( Flux<Record> records, int batchSize )
    {
        this.batchSize = batchSize;
        buffer = new ArrayBlockingQueue<>( batchSize + 1 );
        this.recordSubscriber = new RecordSubscriber();
        records.subscribeWith( recordSubscriber );
    }

    public Record readRecord()
    {
        maybeRequest();

        RecordOrError recordOrError;
        try
        {
            recordOrError = buffer.take();
        }
        catch ( InterruptedException e )
        {
            recordSubscriber.close();
            throw new IllegalStateException( e );
        }
        if ( recordOrError == END )
        {
            return null;
        }

        if ( recordOrError.error != null )
        {
            throw Exceptions.transform( Status.Statement.ExecutionFailed, recordOrError.error );
        }

        return recordOrError.record;
    }

    public void close()
    {
        recordSubscriber.close();
    }

    private void maybeRequest()
    {
        int buffered = buffer.size();
        long pendingRequested = recordSubscriber.pendingRequested.get();
        if ( pendingRequested + buffered == 0 )
        {
            recordSubscriber.request( batchSize );
        }
    }

    private class RecordSubscriber implements Subscriber<Record>
    {

        private volatile Subscription subscription;
        private AtomicLong pendingRequested = new AtomicLong( 0 );

        @Override
        public void onSubscribe( Subscription subscription )
        {
            this.subscription = subscription;
        }

        @Override
        public void onNext( Record record )
        {
            pendingRequested.decrementAndGet();
            buffer.add( new RecordOrError( record, null ) );
        }

        @Override
        public void onError( Throwable throwable )
        {
            buffer.add( new RecordOrError( null, throwable ) );
        }

        @Override
        public void onComplete()
        {
            buffer.add( END );
        }

        void request( long numberOfRecords )
        {
            pendingRequested.addAndGet( numberOfRecords );
            subscription.request( numberOfRecords );
        }

        void close()
        {
            subscription.cancel();
        }
    }

    private static class RecordOrError
    {
        private final Record record;
        private final Throwable error;

        RecordOrError( Record record, Throwable error )
        {
            this.record = record;
            this.error = error;
        }
    }
}
