/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.FakeClockJobScheduler;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;
import static com.neo4j.causalclustering.core.consensus.schedule.TimerServiceTest.Timers.TIMER_A;
import static com.neo4j.causalclustering.core.consensus.schedule.TimerServiceTest.Timers.TIMER_B;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class TimerServiceTest
{
    private final Group group = Group.RAFT_HANDLER;

    private final TimeoutHandler handlerA = mock( TimeoutHandler.class );
    private final TimeoutHandler handlerB = mock( TimeoutHandler.class );

    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final TimerService timerService = new TimerService( scheduler, NullLogProvider.getInstance() );

    private final Timer timerA = timerService.create( TIMER_A, group, handlerA );
    private final Timer timerB = timerService.create( TIMER_B, group, handlerB );

    @Test
    void shouldNotInvokeHandlerBeforeTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1000, MILLISECONDS ) );

        // when
        scheduler.forward( 999, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    void shouldInvokeHandlerOnTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1000, MILLISECONDS ) );

        // when
        scheduler.forward( 1000, MILLISECONDS );

        // then
        verify( handlerA ).onTimeout( any() );
    }

    @Test
    void shouldInvokeHandlerAfterTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 1001, MILLISECONDS );

        // then
        verify( handlerA ).onTimeout( any() );
    }

    @Test
    void shouldInvokeMultipleHandlersOnDifferentTimeouts() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        timerB.set( fixedTimeout( 2, SECONDS ) );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA ).onTimeout( timerA );
        verify( handlerB, never() ).onTimeout( any() );

        // given
        reset( handlerA );
        reset( handlerB );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
        verify( handlerB ).onTimeout( timerB );

        // given
        reset( handlerA );
        reset( handlerB );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
        verify( handlerB, never() ).onTimeout( any() );
    }

    @Test
    void shouldInvokeMultipleHandlersOnSameTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        timerB.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA ).onTimeout( timerA );
        verify( handlerB ).onTimeout( timerB );
    }

    @Test
    void shouldInvokeTimersOnExplicitInvocation() throws Exception
    {
        // when
        timerService.invoke( TIMER_A );

        // then
        verify( handlerA ).onTimeout( timerA );
        verify( handlerB, never() ).onTimeout( any() );

        // given
        reset( handlerA );
        reset( handlerB );

        // when
        timerService.invoke( TIMER_B );

        // then
        verify( handlerA, never() ).onTimeout( any() );
        verify( handlerB ).onTimeout( timerB );
    }

    @Test
    void shouldTimeoutAfterReset() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 900, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );

        // then
        scheduler.forward( 100, MILLISECONDS );

        // when
        verify( handlerA ).onTimeout( any() );
    }

    @Test
    void shouldTimeoutSingleTimeAfterMultipleResets() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 1000, MILLISECONDS );

        // then
        verify( handlerA ).onTimeout( any() );

        // when
        reset( handlerA );
        scheduler.forward( 5000, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    void shouldNotInvokeCancelledTimer() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        scheduler.forward( 900, MILLISECONDS );

        // when
        timerA.cancel( SYNC_WAIT );
        scheduler.forward( 100, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    void shouldNotInvokeDeadTimer() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        scheduler.forward( 900, MILLISECONDS );

        // when
        timerA.kill( SYNC_WAIT );
        scheduler.forward( 100, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    void shouldIgnoreSettingOfDeadTimer() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        scheduler.forward( 900, MILLISECONDS );
        timerA.kill( SYNC_WAIT );

        // when
        boolean wasSet = timerA.set( fixedTimeout( 100, MILLISECONDS ) );
        scheduler.forward( 100, MILLISECONDS );

        // then
        Assertions.assertFalse( wasSet );
        verify( handlerA, never() ).onTimeout( any() );

        // when
        boolean wasReset = timerA.reset();
        scheduler.forward( 1, SECONDS );

        // then
        Assertions.assertFalse( wasReset );
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    void shouldAwaitCancellationUnderRealScheduler() throws Throwable
    {
        // given
        JobScheduler scheduler = createInitialisedScheduler();
        scheduler.start();

        TimerService timerService = new TimerService( scheduler, new Log4jLogProvider( System.out ) );

        CountDownLatch started = new CountDownLatch( 1 );
        CountDownLatch finished = new CountDownLatch( 1 );

        TimeoutHandler handlerA = timer ->
        {
            started.countDown();
            finished.await();
        };

        TimeoutHandler handlerB = timer -> finished.countDown();

        Timer timerA = timerService.create( Timers.TIMER_A, group, handlerA );
        timerA.set( fixedTimeout( 0, SECONDS ) );
        started.await();

        Timer timerB = timerService.create( Timers.TIMER_B, group, handlerB );
        timerB.set( fixedTimeout( 2, SECONDS ) );

        // when
        timerA.cancel( SYNC_WAIT );

        // then
        Assertions.assertEquals( 0, finished.getCount() );

        // cleanup
        scheduler.stop();
        scheduler.shutdown();
    }

    @Test
    void shouldBeAbleToCancelBeforeHandlingWithRealScheduler() throws Throwable
    {
        // given
        JobScheduler scheduler = createInitialisedScheduler();
        scheduler.start();

        TimerService timerService = new TimerService( scheduler, new Log4jLogProvider( System.out ) );

        TimeoutHandler handlerA = timer ->
        {};

        Timer timer = timerService.create( Timers.TIMER_A, group, handlerA );
        timer.set( fixedTimeout( 2, SECONDS ) );

        // when
        timer.cancel( SYNC_WAIT );

        // then: should not deadlock

        // cleanup
        scheduler.stop();
        scheduler.shutdown();
    }

    enum Timers implements TimerService.TimerName
    {
        TIMER_A,
        TIMER_B
    }
}
