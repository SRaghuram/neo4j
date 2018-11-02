/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A robust job catches and logs any exceptions, but keeps running if the job
 * is a recurring one. Any exceptions also end up in the supplied log instead
 * of falling through to syserr. Remaining Throwables (generally errors) are
 * logged but fall through, so that recurring jobs are stopped and the error
 * gets double visibility.
 */
public class RobustJobSchedulerWrapper
{
    private final JobScheduler delegate;
    private final Log log;

    public RobustJobSchedulerWrapper( JobScheduler delegate, Log log )
    {
        this.delegate = delegate;
        this.log = log;
    }

    public JobHandle schedule( Group group, long delayMillis, ThrowingAction<Exception> action )
    {
        return delegate.schedule( group, () -> withErrorHandling( action ), delayMillis, MILLISECONDS );
    }

    public JobHandle scheduleRecurring( Group group, long periodMillis, ThrowingAction<Exception> action )
    {
        return delegate.scheduleRecurring( group, () -> withErrorHandling( action ), periodMillis, MILLISECONDS );
    }

    /**
     * Last line of defense error handling.
     */
    private void withErrorHandling( ThrowingAction<Exception> action )
    {
        try
        {
            action.apply();
        }
        catch ( Exception e )
        {
            log.warn( "Uncaught exception", e );
        }
        catch ( Throwable t )
        {
            log.error( "Uncaught error rethrown", t );
            throw t;
        }
    }
}
