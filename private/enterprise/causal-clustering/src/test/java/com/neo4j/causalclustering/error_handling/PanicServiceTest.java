/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

class PanicServiceTest
{
    private final AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( assertableLogProvider );

    private final SingleThreadExecutor panicExecutor = new SingleThreadExecutor();
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler( panicExecutor );

    private final PanicService panicService = new PanicService( jobScheduler, logService );

    private final NamedDatabaseId namedDatabaseId1 = randomNamedDatabaseId();
    private final NamedDatabaseId namedDatabaseId2 = randomNamedDatabaseId();

    @AfterEach
    void afterEach() throws Exception
    {
        panicExecutor.shutdownNow();
        assertTrue( panicExecutor.awaitTermination( 30, SECONDS ) );
    }

    @Test
    void shouldPanicDatabaseOnce()
    {
        var lockedEventHandler1 = new LockedEventHandler();
        var lockedEventHandler2 = new LockedEventHandler();

        panicService.addPanicEventHandlers( namedDatabaseId1, List.of( lockedEventHandler1 ) );
        panicService.addPanicEventHandlers( namedDatabaseId2, List.of( lockedEventHandler2 ) );

        var panicker1 = panicService.panickerFor( namedDatabaseId1 );

        panicker1.panic( new Exception() );
        panicker1.panic( new IOException() );
        panicker1.panic( new RuntimeException() );

        assertFalse( lockedEventHandler1.isComplete );
        lockedEventHandler1.unlock();

        assertEventually( "Should have completed handling the panic event", () -> lockedEventHandler1.isComplete, TRUE, 30, SECONDS );
        assertEquals( 1, lockedEventHandler1.numberOfPanicEvents.get() );

        assertEquals( 0, lockedEventHandler2.numberOfPanicEvents.get() );
    }

    @Test
    void shouldLogPanicInformation()
    {
        var error = new Exception();
        var panicker = panicService.panickerFor( namedDatabaseId2 );

        panicker.panic( error );

        panicExecutor.awaitBackgroundTaskCompletion();
        assertThat( assertableLogProvider )
                .forClass( panicService.getClass() )
                .forLevel( ERROR )
                .containsMessageWithException( format( "Clustering components for '%s' have encountered a critical error", namedDatabaseId2 ),
                        error );
    }

    @Test
    void shouldIgnoreExceptionsInHandlers()
    {
        var numberOfInvokedHandlers = new AtomicInteger();

        var handlers = List.of(
                new ReportingEventHandler( numberOfInvokedHandlers ),
                new ReportingEventHandler( numberOfInvokedHandlers ),
                new ReportingEventHandler( numberOfInvokedHandlers ),
                new FailingReportingEventHandler( numberOfInvokedHandlers ),
                new FailingReportingEventHandler( numberOfInvokedHandlers ),
                new ReportingEventHandler( numberOfInvokedHandlers ) );

        panicService.addPanicEventHandlers( namedDatabaseId1, handlers );

        var panicker = panicService.panickerFor( namedDatabaseId1 );
        panicker.panic( new Exception() );

        assertEventually( numberOfInvokedHandlers::get, value -> value == handlers.size(), 30, SECONDS );
    }

    @Test
    void shouldExecuteHandlersInOrder()
    {
        var handlerIds = new LinkedBlockingQueue<Integer>();

        var handlers = List.<DatabasePanicEventHandler>of( cause -> handlerIds.add( 1 ), cause -> handlerIds.add( 2 ), cause -> handlerIds.add( 3 ) );

        panicService.addPanicEventHandlers( namedDatabaseId2, handlers );

        var panicker = panicService.panickerFor( namedDatabaseId2 );
        panicker.panic( new Exception() );

        panicExecutor.awaitBackgroundTaskCompletion();

        assertEquals( 1, handlerIds.poll() );
        assertEquals( 2, handlerIds.poll() );
        assertEquals( 3, handlerIds.poll() );
        assertNull( handlerIds.poll() );
    }

    private static class FailingReportingEventHandler extends ReportingEventHandler
    {
        FailingReportingEventHandler( AtomicInteger invocationCounter )
        {
            super( invocationCounter );
        }

        @Override
        public void onPanic( Throwable cause )
        {
            super.onPanic( cause );
            throw new RuntimeException();
        }
    }

    private static class ReportingEventHandler implements DatabasePanicEventHandler
    {
        final AtomicInteger invocationCounter;

        ReportingEventHandler( AtomicInteger invocationCounter )
        {
            this.invocationCounter = invocationCounter;
        }

        @Override
        public void onPanic( Throwable cause )
        {
            invocationCounter.getAndIncrement();
        }
    }

    private static class LockedEventHandler implements DatabasePanicEventHandler
    {
        BinaryLatch latch = new BinaryLatch();
        AtomicInteger numberOfPanicEvents = new AtomicInteger();
        volatile boolean isComplete;

        @Override
        public void onPanic( Throwable cause )
        {
            numberOfPanicEvents.getAndIncrement();
            latch.await();
            isComplete = true;
        }

        void unlock()
        {
            latch.release();
        }
    }

    private static class SingleThreadExecutor extends ThreadPoolExecutor
    {
        SingleThreadExecutor()
        {
            super( 1, 1, 0L, SECONDS, new LinkedBlockingQueue<>(), daemon( PanicServiceTest.class.getSimpleName() ) );
        }

        void awaitBackgroundTaskCompletion()
        {
            assertEventually( this::getCompletedTaskCount, value -> value == 1L, 30, SECONDS );
        }
    }
}
