/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.PartialSummary;
import com.neo4j.fabric.stream.summary.Summary;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.values.virtual.MapValue;

class FabricDriverAsyncTransaction implements FabricDriverTransaction
{
    private final ParameterConverter parameterConverter = new ParameterConverter();

    private final AsyncTransaction asyncTransaction;
    private final AsyncSession asyncSession;
    private final FabricConfig.Graph location;

    FabricDriverAsyncTransaction( AsyncTransaction asyncTransaction, AsyncSession asyncSession, FabricConfig.Graph location )
    {
        this.asyncTransaction = asyncTransaction;
        this.asyncSession = asyncSession;
        this.location = location;
    }

    @Override
    public Mono<Void> commit()
    {
        return Mono.fromFuture( asyncTransaction.commitAsync().toCompletableFuture()).then().doFinally( s -> asyncSession.closeAsync() );
    }

    @Override
    public Mono<Void> rollback()
    {
        return Mono.fromFuture( asyncTransaction.rollbackAsync().toCompletableFuture()).then().doFinally( s -> asyncSession.closeAsync() );
    }

    @Override
    public StatementResult run( String query, MapValue params )
    {
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );
        var statementResultCursor = Mono.fromFuture( asyncTransaction.runAsync( query, paramMap ).toCompletableFuture() );
        return new StatementResultImpl( statementResultCursor, location.getId() );
    }

    private static class StatementResultImpl implements StatementResult
    {

        private final Mono<StatementResultCursor> statementResultCursor;
        private final RecordConverter recordConverter;

        StatementResultImpl( Mono<StatementResultCursor> statementResultCursor, long sourceTag )
        {
            this.statementResultCursor = statementResultCursor;
            this.recordConverter = new RecordConverter( sourceTag );
        }

        @Override
        public Flux<String> columns()
        {
            return statementResultCursor.map( StatementResultCursor::keys ).flatMapMany( Flux::fromIterable );
        }

        @Override
        public Flux<Record> records()
        {
            return statementResultCursor.flatMapMany( cursor -> Flux.from( new RecordPublisher( cursor, recordConverter ) ) );
        }

        @Override
        public Mono<Summary> summary()
        {
            return statementResultCursor
                    .map( StatementResultCursor::summaryAsync )
                    .flatMap( Mono::fromCompletionStage )
                    .map( s -> new PartialSummary( s.counters() ) );
        }
    }

    private static class RecordPublisher implements Publisher<Record>
    {
        private final StatementResultCursor statementResultCursor;
        private final RecordConverter recordConverter;
        private Subscriber<? super Record> subscriber;
        private AtomicBoolean producing = new AtomicBoolean( false );
        private AtomicLong pendingRequests = new AtomicLong();

        RecordPublisher( StatementResultCursor statementResultCursor, RecordConverter recordConverter )
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
}
