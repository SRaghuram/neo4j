/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.impl.query.QueryExecution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Cypher2RxStreamTest
{
    private final AtomicInteger threadCounter = new AtomicInteger();

    private final ExecutorService executorService = Executors.newCachedThreadPool( r -> new Thread( r, "requester-" + threadCounter.incrementAndGet() ) );
    private final List<Thread> requestingThreads = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch firstRequestLatch = new CountDownLatch( 1 );
    private final CountDownLatch completionLatch = new CountDownLatch( 2 );

    @AfterEach
    void afterEach()
    {
        executorService.shutdown();
    }

    @Test
    void testRequestSynchronization() throws Exception
    {
        var queryExecution = mock(QueryExecution.class);

        when( queryExecution.fieldNames() ).thenReturn( new String[] { "a" } );
        when( queryExecution.executionType() ).thenReturn( QueryExecutionType.query( QueryExecutionType.QueryType.READ_ONLY ) );

        AtomicReference<Runnable> secondRequestReference = new AtomicReference<>();
        doAnswer( invocationOnMock ->
        {
            // if this is the first call to 'request', a send call should be triggered
            // a first call is recognized by 'secondRequestReference' holding not null second call reference
            var secondRequest = secondRequestReference.get();
            if ( secondRequest != null )
            {
                secondRequestReference.set( null );
                secondRequest.run();
            }

            requestingThreads.add( Thread.currentThread() );
            // to make sure that the first thread that gets into 'request' will wait there
            firstRequestLatch.await( 5, TimeUnit.SECONDS );
            completionLatch.countDown();
            return null;
        } ).when( queryExecution).request( anyLong() );

        Flux<Record> records = StatementResults.create( querySubscriber -> queryExecution ).records();
        var subscriber = new ConcurrentSubscriber();
        records.subscribeWith( subscriber);
        secondRequestReference.set( subscriber::secondRequest );

        subscriber.firstRequest();

        // wait until 'request' was invoked twice
        assertTrue(completionLatch.await( 5, TimeUnit.SECONDS ));
        assertEquals( 2, requestingThreads.size() );
        assertSame( requestingThreads.get( 0 ), requestingThreads.get( 1 ) );
        assertThat(requestingThreads.get( 0 ).getName() ).contains( "requester" );
    }

    private class ConcurrentSubscriber implements Subscriber<Record>
    {

        Subscription subscription;

        @Override
        public void onSubscribe( Subscription subscription )
        {
            this.subscription = subscription;
        }

        @Override
        public void onNext( Record record )
        {

        }

        @Override
        public void onError( Throwable throwable )
        {
            throwable.printStackTrace();
        }

        @Override
        public void onComplete()
        {

        }

        void firstRequest()
        {
            executorService.submit( () -> subscription.request( 1 ) );
        }

        void secondRequest()
        {
            executorService.submit( () ->
            {
                subscription.request( 1 );
                // unblock the other thread waiting in 'request'
                firstRequestLatch.countDown();
            } );
        }
    }
}
