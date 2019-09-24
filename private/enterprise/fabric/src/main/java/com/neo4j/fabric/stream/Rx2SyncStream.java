/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.stream.summary.Summary;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.api.exceptions.Status;

public class Rx2SyncStream
{

    private static final RecordOrError END = new RecordOrError( null, null );
    private final StatementResult statementResult;
    private final RecordSubscriber recordSubscriber;
    private final int bufferLowWatermark;
    private final int bufferSize;
    private final BlockingQueue<RecordOrError> buffer;
    private final int syncBatchSize;
    private volatile Mode mode = Mode.UNDECIDED;
    private volatile Thread readerThread;

    public Rx2SyncStream( StatementResult statementResult, int bufferLowWatermark, int bufferSize, int syncBatchSize )
    {
        this.statementResult = statementResult;
        this.bufferLowWatermark = bufferLowWatermark;
        this.bufferSize = bufferSize;
        buffer = new ArrayBlockingQueue<>( bufferSize + 1 );
        this.recordSubscriber = new RecordSubscriber();
        this.statementResult.records().subscribeWith( recordSubscriber );
        this.syncBatchSize = syncBatchSize;
    }

    public List<String> getColumns()
    {
        return statementResult.columns().collectList().block();
    }

    public Record readRecord()
    {
        if ( mode == Mode.UNDECIDED )
        {
            initReading();
        }

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
            if ( recordOrError.error instanceof FabricException )
            {
                throw (FabricException) recordOrError.error;
            }
            else
            {
                throw new FabricException( Status.Statement.ExecutionFailed, recordOrError.error );
            }
        }

        return recordOrError.record;
    }

    public Summary summary()
    {
        throw new UnsupportedOperationException( "Statement execution summary is not supported yet" );
    }

    public void close()
    {
        recordSubscriber.close();
    }

    private void initReading()
    {
        readerThread = Thread.currentThread();

        recordSubscriber.request( syncBatchSize );

        // if the reader thread exits the Rx 'request' method and the mode is still undecided,
        // it means that it is not the reader thread that is producing records
        if ( mode == Mode.UNDECIDED )
        {
            mode = Mode.ASYNC;
        }
    }

    private void decideModeOnRecord()
    {
        if ( Thread.currentThread() == readerThread )
        {
            // this means it is the reader thread producing the result in 'request' method
            mode = Mode.SYNC;
        }
        else
        {
            mode = Mode.ASYNC;
        }
    }

    private void maybeRequest()
    {
        int buffered = buffer.size();
        long pendingRequested = recordSubscriber.pendingRequested.get();

        if ( mode == Mode.SYNC )
        {
            if ( pendingRequested + buffered == 0 )
            {
                recordSubscriber.request( syncBatchSize );
            }
        }
        else
        {
            if ( buffered + pendingRequested <= bufferLowWatermark )
            {
                recordSubscriber.request( bufferSize - buffered - pendingRequested );
            }
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
            if ( mode == Mode.UNDECIDED )
            {
                decideModeOnRecord();
            }

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

    /**
     * This stream can operate in 2 modes depending on the behaviour of the stream from which it requests records.
     * The stream, from which this stream requests records can behave synchronously or asynchronously.
     * <p>
     * If it behaves synchronously, it means that it produces records in the 'request' method using the caller thread.
     * For instance, stream that represents the output of Cypher runtime behaves like this.
     * <p>
     * On the other hand, if it behaves asynchronously, the 'request' method returns immediately
     * and the records are produced later by another thread.
     * For instance, stream that represents a result coming from a driver behaves like this.
     */
    private enum Mode
    {
        SYNC,
        ASYNC,
        UNDECIDED
    }
}
