/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

import java.util.concurrent.Executor;

public class CompletionDelegatingOperator extends FluxOperator<Record,Record>
{
    private final Flux<Record> recordStream;
    private final Executor executor;

    public CompletionDelegatingOperator( Flux<Record> recordStream, Executor executor )
    {
        super( recordStream );
        this.recordStream = recordStream;
        this.executor = executor;
    }

    @Override
    public void subscribe( CoreSubscriber downstreamSubscriber )
    {
        recordStream.subscribeWith( new UpstreamSubscriber( downstreamSubscriber ) );
    }

    private class UpstreamSubscriber implements Subscriber<Record>
    {

        private final Subscriber<Record> downstreamSubscriber;

        UpstreamSubscriber( Subscriber<Record> downstreamSubscriber )
        {
            this.downstreamSubscriber = downstreamSubscriber;
        }

        @Override
        public void onSubscribe( Subscription subscription )
        {
            downstreamSubscriber.onSubscribe( subscription );
        }

        @Override
        public void onNext( Record record )
        {
            downstreamSubscriber.onNext( record );
        }

        @Override
        public void onError( Throwable throwable )
        {
            executor.execute( () -> downstreamSubscriber.onError( throwable ) );
        }

        @Override
        public void onComplete()
        {
            executor.execute( downstreamSubscriber::onComplete );
        }
    }
}
