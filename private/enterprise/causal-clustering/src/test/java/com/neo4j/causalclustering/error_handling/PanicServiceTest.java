/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( SuppressOutputExtension.class )
class PanicServiceTest
{
    @Test
    void shouldPanicOnlyOnce() throws Exception
    {
        // given
        PanicService panicService = new PanicService( NullLogProvider.getInstance() );
        LockedEventHandler lockedEventHandler = new LockedEventHandler();
        panicService.addPanicEventHandler( lockedEventHandler );

        // when
        panicService.panic( null );

        // and
        panicService.panic( null );
        panicService.panic( null );
        panicService.panic( null );

        // when
        assertFalse( lockedEventHandler.isComplete );
        lockedEventHandler.unlock();

        // then
        assertEventually( "Should have completed handling the panic event", () -> lockedEventHandler.isComplete, equalTo( true ), 1, TimeUnit.SECONDS );
        assertEquals( 1, lockedEventHandler.atomicInteger.get() );
    }

    @Test
    void shouldLogPanicInformation()
    {
        // given
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        PanicService panicService = new PanicService( assertableLogProvider, true );

        // when
        panicService.panic( null );

        // then
        assertableLogProvider.assertExactly( inLog( panicService.getClass() ).error( PanicService.PANIC_MESSAGE ) );
    }

    @Test
    void shouldLogPanicInformationWithException()
    {
        // given
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        PanicService panicService = new PanicService( assertableLogProvider, true );
        Exception cause = new IllegalStateException();

        // when
        panicService.panic( cause );

        // then
        assertableLogProvider.assertExactly( inLog( panicService.getClass() ).error( equalTo( PanicService.PANIC_MESSAGE ), equalTo( cause ) ) );
    }

    @Test
    void shouldIgnoreExceptionsInEvents()
    {
        // given
        PanicService panicService = new PanicService( NullLogProvider.getInstance(), true );
        AtomicInteger counter = new AtomicInteger();
        List<ReportingEventHandler> events =
                Arrays.asList( new ReportingEventHandler( counter ), new ReportingEventHandler( counter ), new ReportingEventHandler( counter ),
                        new FailingReportingEventHandler( counter ), new FailingReportingEventHandler( counter ), new ReportingEventHandler( counter ) );

        for ( ReportingEventHandler event : events )
        {
            panicService.addPanicEventHandler( event );
        }

        // when
        panicService.panic( null );

        // then
        assertEquals( events.size(), counter.get() );
    }

    @Test
    void shouldExecuteEventsInOrder()
    {
        // given
        PanicService panicService = new PanicService( NullLogProvider.getInstance(), true );
        Queue<Integer> eventIds = new LinkedList<>();
        panicService.addPanicEventHandler( () -> eventIds.add( 1 ) );
        panicService.addPanicEventHandler( () -> eventIds.add( 2 ) );
        panicService.addPanicEventHandler( () -> eventIds.add( 3 ) );

        // when
        panicService.panic( null );

        // then
        assertEquals( new Integer( 1 ), eventIds.poll() );
        assertEquals( new Integer( 2 ), eventIds.poll() );
        assertEquals( new Integer( 3 ), eventIds.poll() );
        assertNull( eventIds.poll() );
    }

    class FailingReportingEventHandler extends ReportingEventHandler
    {
        FailingReportingEventHandler( AtomicInteger atomicInteger )
        {
            super( atomicInteger );
        }

        @Override
        public void onPanic()
        {
            super.onPanic();
            throw new RuntimeException();
        }
    }

    class ReportingEventHandler implements PanicEventHandler
    {
        private final AtomicInteger atomicInteger;

        ReportingEventHandler( AtomicInteger atomicInteger )
        {
            this.atomicInteger = atomicInteger;
        }

        @Override
        public void onPanic()
        {
            atomicInteger.getAndIncrement();
        }
    }

    class LockedEventHandler implements PanicEventHandler
    {
        BinaryLatch latch = new BinaryLatch();
        AtomicInteger atomicInteger = new AtomicInteger( 0 );
        volatile boolean isComplete;

        @Override
        public void onPanic()
        {
            atomicInteger.getAndIncrement();
            latch.await();
            isComplete = true;
        }

        void unlock()
        {
            latch.release();
        }
    }
}
