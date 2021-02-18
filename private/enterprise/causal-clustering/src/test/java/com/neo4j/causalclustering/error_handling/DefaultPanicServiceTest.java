/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.test.OnDemandJobScheduler;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

class DefaultPanicServiceTest
{
    private final AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( assertableLogProvider );
    private final OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler( true );
    private final DefaultPanicService panicService = new DefaultPanicService( jobScheduler, logService, new TestDbmsPanicker() );
    private final NamedDatabaseId namedDatabaseId1 = randomNamedDatabaseId();
    private final NamedDatabaseId namedDatabaseId2 = randomNamedDatabaseId();

    @AfterEach
    void cleanup()
    {
        Assertions.assertThat( jobScheduler.getJob() ).as( "There are no remaining queued tasks on the scheduler" ).isNull();
        jobScheduler.close();
    }

    @Test
    void shouldPanicDatabaseOnce()
    {
        var lockedEventHandler1 = new LockedEventHandler();
        var lockedEventHandler2 = new LockedEventHandler();

        panicService.addDatabasePanicEventHandlers( namedDatabaseId1, List.of( lockedEventHandler1 ) );
        panicService.addDatabasePanicEventHandlers( namedDatabaseId2, List.of( lockedEventHandler2 ) );

        var panicker1 = panicService.panickerFor( namedDatabaseId1 );

        panicker1.panic( DatabasePanicReason.TEST, new Exception() );
        panicker1.panic( DatabasePanicReason.TEST, new IOException() );
        panicker1.panic( DatabasePanicReason.TEST, new RuntimeException() );

        assertFalse( lockedEventHandler1.isComplete );

        runBackgroundTasks();

        assertThat( lockedEventHandler1.isComplete ).as( "Should have completed handling the panic event" ).isTrue();
        assertEquals( 1, lockedEventHandler1.numberOfPanicEvents.get() );
        assertEquals( 0, lockedEventHandler2.numberOfPanicEvents.get() );
    }

    @Test
    void shouldLogPanicInformation()
    {
        var error = new Exception();
        var panicker = panicService.panickerFor( namedDatabaseId2 );

        panicker.panic( DatabasePanicReason.TEST, error );

        runBackgroundTasks();
        assertThat( assertableLogProvider )
                .forClass( panicService.getClass() )
                .forLevel( ERROR )
                .containsMessageWithException( format( "Components for '%s' have encountered a critical error", namedDatabaseId2 ),
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

        panicService.addDatabasePanicEventHandlers( namedDatabaseId1, handlers );

        var panicker = panicService.panickerFor( namedDatabaseId1 );
        panicker.panic( DatabasePanicReason.TEST, new Exception() );

        runBackgroundTasks();
        assertThat( numberOfInvokedHandlers.get() ).isEqualTo( handlers.size() );
    }

    @Test
    void shouldExecuteHandlersInOrder()
    {
        var handlerIds = new LinkedBlockingQueue<Integer>();

        var handlers = List.<DatabasePanicEventHandler>of(
                info -> handlerIds.add( 1 ), info -> handlerIds.add( 2 ), info -> handlerIds.add( 3 )
        );

        panicService.addDatabasePanicEventHandlers( namedDatabaseId2, handlers );

        var panicker = panicService.panickerFor( namedDatabaseId2 );
        panicker.panic( DatabasePanicReason.TEST, new Exception() );

        runBackgroundTasks();

        assertEquals( 1, handlerIds.poll() );
        assertEquals( 2, handlerIds.poll() );
        assertEquals( 3, handlerIds.poll() );
        assertNull( handlerIds.poll() );
    }

    @Test
    void shouldPanicDbms()
    {
        var testException = new Exception( "something went wrong" );
        panicService.panicker().panic( new DbmsPanicEvent( DbmsPanicReason.Test, testException ) );

        runBackgroundTasks();
        assertThat( panicCounter.get() ).isEqualTo( 1 );
        assertThat( lastPanicReason ).isEqualTo( DbmsPanicReason.Test );
        assertThat( lastPanicException ).isEqualTo( testException );
    }

    private void runBackgroundTasks()
    {
        while ( jobScheduler.getJob() != null )
        {
            jobScheduler.runJob();
        }
    }

    private static class FailingReportingEventHandler extends ReportingEventHandler
    {
        FailingReportingEventHandler( AtomicInteger invocationCounter )
        {
            super( invocationCounter );
        }

        @Override
        public void onPanic( DatabasePanicEvent event )
        {
            super.onPanic( event );
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
        public void onPanic( DatabasePanicEvent event )
        {
            invocationCounter.getAndIncrement();
        }
    }

    private static class LockedEventHandler implements DatabasePanicEventHandler
    {
        AtomicInteger numberOfPanicEvents = new AtomicInteger();
        volatile boolean isComplete;

        @Override
        public void onPanic( DatabasePanicEvent event )
        {
            numberOfPanicEvents.getAndIncrement();
            isComplete = true;
        }
    }

    private final AtomicInteger panicCounter = new AtomicInteger();
    private volatile Throwable lastPanicException;
    private volatile Panicker.Reason lastPanicReason;

    private class TestDbmsPanicker implements DbmsPanicker
    {
        @Override
        public void panic( DbmsPanicEvent event )
        {
            panicCounter.incrementAndGet();
            lastPanicException = event.getCause();
            lastPanicReason = event.getReason();
        }
    }
}
