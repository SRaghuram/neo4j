/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;

class RecordPublisher implements Publisher<Record>
{
    private final ResultCursor statementResultCursor;
    private final RecordConverter recordConverter;
    private Subscriber<? super Record> subscriber;
    private AtomicBoolean producing = new AtomicBoolean( false );
    private AtomicLong pendingRequests = new AtomicLong();

    RecordPublisher( ResultCursor statementResultCursor, RecordConverter recordConverter )
    {
        this.statementResultCursor = statementResultCursor;
        this.recordConverter = recordConverter;
    }

    @Override
    public void subscribe( Subscriber<? super Record> subscriber )
    {
        this.subscriber = subscriber;
        subscriber.onSubscribe( new Subscription()
        {
            @Override
            public void request( long l )
            {
                pendingRequests.addAndGet( l );
                maybeProduce();
            }

            @Override
            public void cancel()
            {
                statementResultCursor.consumeAsync();
            }
        } );
    }

    private void maybeProduce()
    {

        if ( pendingRequests.get() == 0 || !producing.compareAndSet( false, true ) )
        {
            return;
        }

        produce();
    }

    private void produce()
    {
        var recordFuture = statementResultCursor.nextAsync();

        recordFuture.whenComplete( ( record, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                subscriber.onError( error );
                return;
            }

            if ( record == null )
            {
                subscriber.onComplete();
                return;
            }

            var pending = pendingRequests.decrementAndGet();
            try
            {
                var convertedRecord = Records.lazy( record.size(),
                        () -> Records.of( record.values().stream().map( recordConverter::convertValue ).collect( Collectors.toList() ) ) );
                subscriber.onNext( convertedRecord );
            }
            catch ( Throwable actionError )
            {
                subscriber.onError( actionError );
                return;
            }

            if ( pending > 0 )
            {
                produce();
                return;
            }

            producing.set( false );
            maybeProduce();
        } );
    }
}
