/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;

/**
 * A timer which can be set to go off at a future point in time.
 * <p>
 * When the timer goes off a timeout event is said to occur and the registered
 * {@link TimeoutHandler} will be invoked.
 */
public class Timer
{
    private final TimerService.TimerName name;
    private final JobScheduler scheduler;
    private final Log log;
    private final Group group;
    private final TimeoutHandler handler;

    private Timeout timeout;
    private Delay delay;
    private JobHandle job;
    private long activeJobId;
    private boolean isDead;
    private volatile boolean isRunning;

    /**
     * Creates a timer in the deactivated state.
     *
     * @param name The name of the timer.
     * @param scheduler The underlying scheduler used.
     * @param group The scheduler group used.
     * @param handler The timeout handler.
     */
    protected Timer( TimerService.TimerName name, JobScheduler scheduler, Log log, Group group, TimeoutHandler handler )
    {
        this.name = name;
        this.scheduler = scheduler;
        this.log = log;
        this.group = group;
        this.handler = handler;
    }

    /**
     * Activates the timer to go off at the specified timeout. Calling this method
     * when the timer already is active will shift the timeout to the new value.
     *
     * @param newTimeout The new timeout value.
     * @return false if the timer is dead and thus cannot be set, otherwise true.
     */
    public synchronized boolean set( Timeout newTimeout )
    {
        if ( isDead )
        {
            return false;
        }
        delay = newTimeout.next();
        timeout = newTimeout;
        long jobId = newJobId();
        job = scheduler.schedule( group, () -> handle( jobId ), delay.amount(), delay.unit() );
        return true;
    }

    private long newJobId()
    {
        activeJobId = activeJobId + 1;
        return activeJobId;
    }

    private void handle( long jobId )
    {
        synchronized ( this )
        {
            if ( activeJobId != jobId )
            {
                return;
            }
            else
            {
                isRunning = true;
            }
        }

        try
        {
            handler.onTimeout( this );
        }
        catch ( Throwable e )
        {
            log.error( format( "[%s] Handler threw exception", canonicalName() ), e );
        }
        finally
        {
            isRunning = false;
        }
    }

    /**
     * Resets the timer based on the currently programmed timeout.
     *
     * @return false if the timer is dead and thus cannot be set, otherwise true.
     */
    public synchronized boolean reset()
    {
        if ( timeout == null )
        {
            throw new IllegalStateException( "You can't reset until you have set a timeout" );
        }
        return set( timeout );
    }

    /**
     * Deactivates the timer and cancels a currently running job.
     * <p>
     * Be careful to not have a timeout handler executing in parallel with a
     * timer, because this will just cancel the timer. If you for example
     * {@link #reset()} in the timeout handler, but keep executing the handler,
     * then a subsequent cancel will not ensure that the first execution of the
     * handler was cancelled.
     *
     * @param cancelMode The mode of cancelling.
     */
    public void cancel( CancelMode cancelMode )
    {
        JobHandle job;

        synchronized ( this )
        {
            activeJobId++;
            job = isRunning ? this.job : null;
        }

        if ( job != null )
        {
            try
            {
                if ( cancelMode == CancelMode.SYNC_WAIT )
                {
                    job.waitTermination();
                }
                else if ( cancelMode == CancelMode.ASYNC_INTERRUPT )
                {
                    job.cancel();
                }
            }
            catch ( Exception e )
            {
                log.warn( format( "[%s] Cancelling timer threw exception", canonicalName() ), e );
            }
        }
    }

    /**
     * Kills a timer and cancels any current job.
     *
     * A dead timer can no longer be set/reset, and those invocations will be silently ignored.
     *
     * @param cancelMode The mode of cancelling.
     */
    public void kill( CancelMode cancelMode )
    {
        synchronized ( this )
        {
            isDead = true;
        }
        cancel( cancelMode );
    }

    /**
     * Schedules the timer for an immediate timeout.
     */
    public synchronized void invoke()
    {
        long jobId = newJobId();
        job = scheduler.schedule( group, () -> handle( jobId ) );
    }

    synchronized Delay delay()
    {
        return delay;
    }

    public TimerService.TimerName name()
    {
        return name;
    }

    private String canonicalName()
    {
        return name.getClass().getCanonicalName() + "." + name.name();
    }

    public enum CancelMode
    {
        /**
         * Asynchronously cancels.
         */
        ASYNC,

        /**
         * Asynchronously cancels and interrupts the handler.
         */
        ASYNC_INTERRUPT,

        /**
         * Synchronously cancels and waits for the handler to finish.
         */
        SYNC_WAIT,

        /*
         * Note that SYNC_INTERRUPT cannot be supported, since the underlying
         * primitive is a future which cannot be cancelled/interrupted and awaited.
         */
    }
}
