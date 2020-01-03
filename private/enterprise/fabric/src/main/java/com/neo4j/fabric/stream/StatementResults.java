/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.stream.summary.Summary;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

public class StatementResults
{

    public static StatementResult map( StatementResult statementResult, Function<Flux<Record>,Flux<Record>> func )
    {
        return new BasicStatementResult( statementResult.columns(), func.apply( statementResult.records() ), statementResult.summary() );
    }

    public static StatementResult initial()
    {
        return new BasicStatementResult( Flux.empty(), Flux.just( Records.empty() ), Mono.empty() );
    }

    public static StatementResult create( Function<QuerySubscriber,QueryExecution> execution )
    {
        try
        {
            QuerySubject querySubject = new QuerySubject();
            QueryExecution queryExecution = execution.apply( querySubject );
            querySubject.setQueryExecution( queryExecution );
            return create(
                    Flux.fromArray( queryExecution.fieldNames() ),
                    Flux.from( querySubject ),
                    Mono.empty()
            );
        }
        catch ( RuntimeException re )
        {
            return error( re );
        }
    }

    public static StatementResult create( Flux<String> columns, Flux<Record> records, Mono<Summary> summary )
    {
        return new BasicStatementResult( columns, records, summary );
    }

    public static StatementResult error( Throwable err )
    {
        return new BasicStatementResult( Flux.error( err ), Flux.error( err ), Mono.error( err ) );
    }

    public static StatementResult trace( StatementResult input )
    {
        return new BasicStatementResult(
                input.columns(),
                input.records().doOnEach( signal ->
                {
                    if ( signal.hasValue() )
                    {
                        System.out.println( String.join( ", ", signal.getType().toString(), Records.show( signal.get() ) ) );
                    }
                    else if ( signal.hasError() )
                    {
                        System.out.println( String.join( ", ", signal.getType().toString(), signal.getThrowable().toString() ) );
                    }
                    else
                    {
                        System.out.println( String.join( ", ", signal.getType().toString() ) );
                    }
                } ),
                input.summary()
        );
    }

    private static class BasicStatementResult implements StatementResult
    {
        private final Flux<String> columns;
        private final Flux<Record> records;
        private final Mono<Summary> summary;

        BasicStatementResult( Flux<String> columns, Flux<Record> records, Mono<Summary> summary )
        {
            this.columns = columns;
            this.records = records;
            this.summary = summary;
        }

        @Override
        public Flux<String> columns()
        {
            return columns;
        }

        @Override
        public Flux<Record> records()
        {
            return records;
        }

        @Override
        public Mono<Summary> summary()
        {
            return summary;
        }
    }

    private static class QuerySubject extends RecordQuerySubscriber implements Publisher<Record>
    {
        private Subscriber<? super Record> subscriber;
        private QueryExecution queryExecution;
        private Throwable cachedError;
        private boolean errorReceived;

        void setQueryExecution( QueryExecution queryExecution )
        {
            this.queryExecution = queryExecution;
        }

        @Override
        public void onNext( Record record )
        {
            subscriber.onNext( record );
        }

        @Override
        public void onError( Throwable throwable )
        {
            errorReceived = true;

            if ( subscriber == null )
            {
                cachedError = throwable;
            }
            else
            {
                subscriber.onError( throwable );
            }
        }

        @Override
        public void onResultCompleted( QueryStatistics statistics )
        {
            subscriber.onComplete();
        }

        @Override
        public void subscribe( Subscriber<? super Record> subscriber )
        {

            if ( this.subscriber != null )
            {
                throw new FabricException( Status.General.UnknownError, "Already subscribed" );
            }
            this.subscriber = subscriber;
            Subscription subscription = new Subscription()
            {

                @Override
                public void request( long size )
                {
                    maybeSendCachedError();
                    try
                    {
                        queryExecution.request( size );

                        // If 'await' is called after an error has been received, it will throw with the same error.
                        // Reactor operators don't like when 'onError' is called more than once. Typically, the second call throws an exception,
                        // which can have a disastrous effect on the RX pipeline
                        if ( !errorReceived )
                        {
                            queryExecution.await();
                        }
                    }
                    catch ( Exception e )
                    {
                        subscriber.onError( e );
                    }
                }

                @Override
                public void cancel()
                {
                    queryExecution.cancel();
                }
            };
            subscriber.onSubscribe( subscription );
            maybeSendCachedError();
        }

        private void maybeSendCachedError()
        {

            if ( cachedError != null )
            {
                subscriber.onError( cachedError );
                cachedError = null;
            }
        }
    }

    private abstract static class RecordQuerySubscriber implements QuerySubscriber
    {
        private int numberOfFields;
        private AnyValue[] fields;

        @Override
        public void onResult( int numberOfFields )
        {
            this.numberOfFields = numberOfFields;
        }

        @Override
        public void onRecord()
        {
            fields = new AnyValue[numberOfFields];
        }

        @Override
        public void onField( int offset, AnyValue value )
        {
            fields[offset] = value;
        }

        @Override
        public void onRecordCompleted()
        {
            onNext( Records.of( fields ) );
        }

        abstract void onNext( Record record );
    }
}
