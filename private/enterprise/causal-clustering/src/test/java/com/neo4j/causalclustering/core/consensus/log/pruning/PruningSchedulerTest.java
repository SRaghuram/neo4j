/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.pruning;

import com.neo4j.causalclustering.core.state.RaftLogPruner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class PruningSchedulerTest
{
    private final RaftLogPruner logPruner = mock( RaftLogPruner.class );
    private final OnDemandJobScheduler jobScheduler = spy( new OnDemandJobScheduler() );

    @Test
    void shouldScheduleTheCheckPointerJobOnStart()
    {
        // given
        var scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

        assertNull( jobScheduler.getJob() );

        // when
        scheduler.start();

        // then
        assertNotNull( jobScheduler.getJob() );
        verify( jobScheduler ).schedule( eq( Group.RAFT_LOG_PRUNING ), any( Runnable.class ),
                eq( 20L ), eq( TimeUnit.MILLISECONDS ) );
    }

    @Test
    void shouldRescheduleTheJobAfterARun() throws Throwable
    {
        // given
        var scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        var scheduledJob = jobScheduler.getJob();
        assertNotNull( scheduledJob );

        // when
        jobScheduler.runJob();

        // then
        verify( jobScheduler, times( 2 ) ).schedule( eq( Group.RAFT_LOG_PRUNING ), any( Runnable.class ),
                eq( 20L ), eq( TimeUnit.MILLISECONDS ) );
        verify( logPruner ).prune();
        assertEquals( scheduledJob, jobScheduler.getJob() );
    }

    @Test
    void shouldNotRescheduleAJobWhenStopped()
    {
        // given
        var scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        assertNotNull( jobScheduler.getJob() );

        // when
        scheduler.stop();

        // then
        assertNull( jobScheduler.getJob() );
    }

    @Test
    void stoppedJobCantBeInvoked() throws Throwable
    {
        var scheduler = new PruningScheduler( logPruner, jobScheduler, 10L, NullLogProvider.getInstance() );
        scheduler.start();
        jobScheduler.runJob();

        // verify checkpoint was triggered
        verify( logPruner ).prune();

        // simulate scheduled run that was triggered just before stop
        scheduler.stop();
        scheduler.start();
        jobScheduler.runJob();

        // logPruner should not be invoked now because job stopped
        verifyNoMoreInteractions( logPruner );
    }

    @Test
    @Timeout( 5 )
    void shouldWaitOnStopUntilTheRunningCheckpointIsDone() throws Throwable
    {
        // given
        final var ex = new AtomicReference<Throwable>();
        final var checkPointerLatch = new DoubleLatch( 1 );
        var logPruner = new RaftLogPruner( null, null )
        {
            @Override
            public void prune()
            {
                checkPointerLatch.startAndWaitForAllToStart();
                checkPointerLatch.waitForAllToFinish();
            }
        };

        final var scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

        // when
        scheduler.start();

        var runCheckPointer = new Thread( jobScheduler::runJob );
        runCheckPointer.start();

        checkPointerLatch.waitForAllToStart();

        var stopper = new Thread( () -> {
            try
            {
                scheduler.stop();
            }
            catch ( Throwable throwable )
            {
                ex.set( throwable );
            }
        } );

        stopper.start();

        checkPointerLatch.finish();
        runCheckPointer.join();

        stopper.join();

        assertNull( ex.get() );
    }
}
