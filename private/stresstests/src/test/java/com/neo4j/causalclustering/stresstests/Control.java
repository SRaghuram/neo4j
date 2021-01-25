/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.neo4j.internal.utils.DumpUtils;
import org.neo4j.logging.Log;
import org.neo4j.util.concurrent.Futures;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.function.Suppliers.untilTimeExpired;
import static org.neo4j.internal.helpers.Exceptions.findCauseOrSuppressed;

public class Control
{
    private final AtomicBoolean stopTheWorld = new AtomicBoolean();
    private final BooleanSupplier keepGoing;
    private final Log log;
    private final long totalDurationMinutes;
    private Throwable failure;

    public Control( Config config )
    {
        this.log = config.logProvider().getLog( getClass() );
        long workDurationMinutes = config.workDurationMinutes();
        this.totalDurationMinutes = workDurationMinutes + config.shutdownDurationMinutes();

        BooleanSupplier notExpired = untilTimeExpired( workDurationMinutes, MINUTES );
        this.keepGoing = () -> !stopTheWorld.get() && notExpired.getAsBoolean();
    }

    public boolean keepGoing()
    {
        return keepGoing.getAsBoolean();
    }

    public synchronized void onFailure( Throwable cause )
    {
        if ( !keepGoing() && findCauseOrSuppressed( cause, t -> t instanceof InterruptedException ).isPresent() )
        {
            log.info( "Ignoring interrupt at end of test", cause );
            return;
        }

        if ( failure == null )
        {
            failure = cause;
        }
        else
        {
            failure.addSuppressed( cause );
        }
        log.error( "Failure occurred", cause );
        log.error( "Thread dump always printed on failure" );
        threadDump();
        stopTheWorld.set( true );
    }

    public synchronized void assertNoFailure()
    {
        if ( failure != null )
        {
            throw new RuntimeException( "Test failed", failure );
        }
    }

    public void awaitEnd( Iterable<Future<?>> completions ) throws InterruptedException, TimeoutException, ExecutionException
    {
        Futures.combine( completions ).get( totalDurationMinutes, MINUTES );
    }

    private void threadDump()
    {
        log.info( DumpUtils.threadDump() );
    }

    void onFinish()
    {
        stopTheWorld.set( true );
    }
}
