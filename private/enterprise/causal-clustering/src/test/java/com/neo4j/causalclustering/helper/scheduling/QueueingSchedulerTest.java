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
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.Group;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

class QueueingSchedulerTest
{
    private final ThreadPoolJobScheduler executorService = new ThreadPoolJobScheduler( Executors.newSingleThreadExecutor() );
    private final UnboundedJobsQueue jobsQueue = new UnboundedJobsQueue();
    private final QueueingScheduler scheduler =
            new QueueingScheduler( executorService, Group.CORE_STATE_APPLIER, NullLog.getInstance(), jobsQueue );

    @AfterEach
    void shutdown()
    {
        scheduler.disable();
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
    void shouldClearQueueOnDisable()
    {
        // given
        AtomicInteger integer = new AtomicInteger();
        CountDownLatch countDownLatch = new CountDownLatch( 1 );

        // when
        scheduler.offerJob( () -> waitOnLatch( countDownLatch ) );
        scheduler.offerJob( () -> integer.set( 1 ) );

        // then
        assertEquals( 1, jobsQueue.queue.size() );

        // when
        runAsync( scheduler::disable );

        // then
        assertEventually( jobsQueue.queue::isEmpty, TRUE, 1, MINUTES );

        // and
        countDownLatch.countDown();
        assertEquals( 0, integer.get() );
    }

    @Test
    void shouldNotAllowSchedulingOfJobsAfterBeingDisabled()
    {
        // given
        AtomicInteger integer = new AtomicInteger();
        CountDownLatch countDownLatch = new CountDownLatch( 1 );

        // when
        scheduler.offerJob( () -> waitOnLatch( countDownLatch ) );
        scheduler.offerJob( () -> integer.set( 1 ) );

        // and
        runAsync( scheduler::disable );

        // when queue has been cleared we know that we are disabled
        assertEventually( jobsQueue.queue::isEmpty, TRUE, 1, MINUTES );

        // then any offered should throw exception
        assertThrows( IllegalStateException.class, () -> scheduler.offerJob( () -> integer.set( 2 ) ) );

        countDownLatch.countDown();
        assertEquals( 0, integer.get() );
    }

    @Test
    void shouldScheduleJobsInOrder() throws ExecutionException, InterruptedException
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
            var scheduler = new QueueingScheduler( threadPoolJobScheduler, Group.CORE_STATE_APPLIER, NullLog.getInstance(), jobsQueue );

            // when
            scheduler.offerJob( incrementingJob( integer, firstLatch ) );
            scheduler.offerJob( incrementingJob( integer, firstLatch ) );
            scheduler.offerJob( integer::incrementAndGet );
            scheduler.offerJob( integer::incrementAndGet );
            scheduler.offerJob( () -> futureOne.complete( new Object() ) );

            scheduler.offerJob( incrementingJob( integer, secondLatch ) );
            scheduler.offerJob( integer::incrementAndGet );

            // then: nothing should have run, because we are waiting for first latch
            assertEquals( 0, integer.get() );

            // when
            firstLatch.countDown();
            futureOne.get();

            // then: we should have run up to those jobs waiting for the second latch
            assertEquals( 4, integer.get() );

            // when
            secondLatch.countDown();
            scheduler.offerJob( () -> futureTwo.complete( new Object() ) );

            // then
            futureTwo.get();
            assertEquals( 6, integer.get() );
        }
        finally
        {
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

    private void waitOnLatch( CountDownLatch countDownLatch )
    {
        try
        {
            countDownLatch.await();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
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
