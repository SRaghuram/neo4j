/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.Group;
import org.neo4j.test.scheduler.JobSchedulerAdapter;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

class QueueingSchedulerTest
{
    private final ThreadPoolJobScheduler executorService = new ThreadPoolJobScheduler( Executors.newSingleThreadExecutor() );
    private final UnboundedJobsQueue jobsQueue = new UnboundedJobsQueue();
    private final QueueingScheduler scheduler =
            new QueueingScheduler( executorService, Group.CORE_STATE_APPLIER,
                                   NullLog.getInstance(), 1, jobsQueue );

    @AfterEach
    void shutdown()
    {
        executorService.shutdown();
    }

    @Test
    void shouldRunScheduledJobs() throws ExecutionException, InterruptedException
    {
        // given
        var future = new CompletableFuture<>();

        // when
        scheduler.offerJob( () -> future.complete( 1 ) );

        // then
        assertEquals( 1, future.get() );
    }

    @Test
    void shouldClearQueueOnAbort()
    {
        // given
        AtomicInteger integer = new AtomicInteger();
        CountDownLatch countDownLatch = new CountDownLatch( 1 );

        // when
        scheduler.offerJob( () ->
                            {
                                try
                                {
                                    countDownLatch.await();
                                }
                                catch ( InterruptedException e )
                                {
                                    throw new RuntimeException( e );
                                }
                            } );
        scheduler.offerJob( () -> integer.set( 1 ) );

        // then
        assertEquals( 1, jobsQueue.queue.size() );

        // when
        scheduler.abortAll();

        // then
        assertTrue( jobsQueue.queue.isEmpty() );

        // and
        countDownLatch.countDown();
        scheduler.stopAll();
        assertEquals( 0, integer.get() );
    }

    @Test
    void shouldNotScheduleMoreJobsWhenStopping() throws ExecutionException, InterruptedException
    {
        // given
        AtomicInteger integer = new AtomicInteger();
        CountDownLatch countDownLatch = new CountDownLatch( 1 );
        var future = new CompletableFuture<>();
        var stopThread = new Thread( () ->
                                     {
                                         scheduler.stopAll();
                                         future.complete( new Object() );
                                     } );

        // when
        scheduler.offerJob( () ->
                            {
                                try
                                {
                                    countDownLatch.await();
                                }
                                catch ( InterruptedException e )
                                {
                                    throw new RuntimeException( e );
                                }
                            } );
        scheduler.offerJob( () -> integer.set( 1 ) );

        // and
        stopThread.start();

        try
        {

            // when queue has been cleared we know that we have stopped.
            assertEventually( jobsQueue.queue::isEmpty, TRUE, 5, TimeUnit.SECONDS );

            // then any offered should be ignored
            scheduler.offerJob( () -> integer.set( 2 ) );
            // still stopped
            assertFalse( future.isDone() );
        }
        finally
        {
            countDownLatch.countDown();
            stopThread.join();
            future.get();
        }

        assertEquals( 0, integer.get() );
    }

    @Test
    void shouldScheduleJobsAsExpected() throws ExecutionException, InterruptedException
    {
        var integer = new AtomicInteger();
        var futureOne = new CompletableFuture<>();
        var futureTwo = new CompletableFuture<>();
        var firstLatch = new CountDownLatch( 1 );
        var secondLatch = new CountDownLatch( 1 );
        var jobsQueue = new UnboundedJobsQueue();

        var threadPoolJobScheduler = new ThreadPoolJobScheduler( Executors.newCachedThreadPool() );
        try
        {
            var scheduler =
                    new QueueingScheduler( threadPoolJobScheduler, Group.CORE_STATE_APPLIER, NullLog.getInstance(),
                                           2, jobsQueue );

            // when
            scheduler.offerJob( incrementingJob( integer, firstLatch ) );
            scheduler.offerJob( incrementingJob( integer, firstLatch ) );
            scheduler.offerJob( incrementingJob( integer, secondLatch ) );
            scheduler.offerJob( integer::incrementAndGet );
            scheduler.offerJob( integer::incrementAndGet );
            scheduler.offerJob( () -> futureOne.complete( new Object() ) );

            // then
            assertEquals( 4, jobsQueue.queue.size() );

            // when
            firstLatch.countDown();
            futureOne.get();

            // then
            assertEquals( 0, jobsQueue.queue.size() );

            // when
            scheduler.offerJob( integer::incrementAndGet );
            scheduler.offerJob( () -> futureTwo.complete( new Object() ) );

            // then
            futureTwo.get();
            assertEquals( 5, integer.get() );
        }
        finally
        {
            secondLatch.countDown();
            threadPoolJobScheduler.shutdown();
        }
    }

    @Test
    void shouldReleaseCompletedJobs() throws ExecutionException, InterruptedException
    {
        // given
        AtomicInteger integer = new AtomicInteger();
        var future = new CompletableFuture<>();

        // when
        scheduler.offerJob( () -> integer.set( 1 ) );
        scheduler.offerJob( () -> integer.set( 2 ) );
        scheduler.offerJob( () -> integer.set( 3 ) );
        scheduler.offerJob( () -> future.complete( new Object() ) );

        future.get();

        // then
        assertEquals( 3, integer.get() );
    }

    @Test
    void shouldOnlyAllowPositiveScheduledSize()
    {
        assertThrows( IllegalArgumentException.class,
                      () -> new QueueingScheduler( new JobSchedulerAdapter(), Group.CORE_STATE_APPLIER, NullLog.getInstance(), 0,
                                                   new SingleElementJobsQueue<>() ) );
    }

    private Runnable incrementingJob( AtomicInteger integer, CountDownLatch latch )
    {
        return () ->
        {
            try
            {
                latch.await();
                integer.incrementAndGet();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private static class UnboundedJobsQueue implements JobsQueue<Runnable>
    {
        private final Queue<Runnable> queue = new LinkedList<>();

        @Override
        public Runnable poll()
        {
            return queue.poll();
        }

        @Override
        public void offer( Runnable element )
        {
            queue.offer( element );
        }

        @Override
        public void clear()
        {
            queue.clear();
        }
    }
}
