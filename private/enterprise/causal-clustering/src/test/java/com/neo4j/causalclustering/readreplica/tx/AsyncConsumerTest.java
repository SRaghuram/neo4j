/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AsyncConsumerTest
{
    @Test
    void shouldCompletedOutstandingJobsWhenStopped()
    {
        var queue = new LinkedBlockingQueue<Runnable>();
        var asyncConsumer = new AsyncConsumer( queue, NullLog.getInstance() );
        var counter = new MutableInt();

        queue.add( counter::increment );
        queue.add( counter::increment );
        queue.add( asyncConsumer::stopPolling );
        queue.add( counter::increment );

        asyncConsumer.run();

        assertThat( counter.getValue() ).isEqualTo( 3 );
    }

    @Test
    void shouldWaitForJob() throws InterruptedException
    {
        var queue = new LinkedBlockingQueue<Runnable>();
        var asyncConsumer = new AsyncConsumer( queue, NullLog.getInstance() );
        var beforeStart = new CountDownLatch( 1 );
        var afterStart = new CountDownLatch( 1 );

        queue.add( beforeStart::countDown );

        var applierThread = new Thread( asyncConsumer );
        applierThread.start();
        beforeStart.await();

        queue.add( afterStart::countDown );
        afterStart.await();

        asyncConsumer.stopPolling();
        applierThread.join();
    }

    @Test
    void shouldLogUnexpectedError() throws InterruptedException
    {
        var queue = new LinkedBlockingQueue<Runnable>();
        var mockedLog = mock( Log.class );
        var asyncConsumer = new AsyncConsumer( queue, mockedLog );

        queue.add( () ->
        {
            throw new RuntimeException( "Problem" );
        } );

        var applierThread = new Thread( asyncConsumer );
        applierThread.start();
        asyncConsumer.stopPolling();
        applierThread.join();

        verify( mockedLog ).error( anyString(), any( Exception.class ) );
    }
}
