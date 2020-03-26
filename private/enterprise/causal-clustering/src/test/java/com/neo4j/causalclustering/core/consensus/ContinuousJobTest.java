/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.lessThan;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class ContinuousJobTest
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;
    private final JobScheduler scheduler = createInitialisedScheduler();

    @Test
    void shouldRunJobContinuously() throws Throwable
    {
        // given
        CountDownLatch latch = new CountDownLatch( 10 );
        Runnable task = latch::countDown;

        ContinuousJob continuousJob =
                new ContinuousJob( scheduler.threadFactory( Group.RAFT_BATCH_HANDLER ), task, NullLogProvider.getInstance() );

        // when
        try ( Lifespan ignored = new Lifespan( scheduler, continuousJob ) )
        {
            //then
            Assertions.assertTrue( latch.await( DEFAULT_TIMEOUT_MS, MILLISECONDS ) );
        }
    }

    @Test
    void shouldTerminateOnStop() throws Exception
    {
        // given: this task is gonna take >20 ms total
        Semaphore semaphore = new Semaphore( -20 );

        Runnable task = () ->
        {
            LockSupport.parkNanos( 1_000_000 ); // 1 ms
            semaphore.release();
        };

        ContinuousJob continuousJob =
                new ContinuousJob( scheduler.threadFactory( Group.RAFT_BATCH_HANDLER ), task, NullLogProvider.getInstance() );

        // when
        long startTime = System.currentTimeMillis();
        try ( Lifespan ignored = new Lifespan( scheduler, continuousJob ) )
        {
            semaphore.acquireUninterruptibly();
        }
        long runningTime = System.currentTimeMillis() - startTime;

        // then
        MatcherAssert.assertThat( runningTime, lessThan( DEFAULT_TIMEOUT_MS ) );

        //noinspection StatementWithEmptyBody
        while ( semaphore.tryAcquire() )
        {
            // consume all outstanding permits
        }

        // no more permits should be granted
        semaphore.tryAcquire( 10, MILLISECONDS );
    }
}
