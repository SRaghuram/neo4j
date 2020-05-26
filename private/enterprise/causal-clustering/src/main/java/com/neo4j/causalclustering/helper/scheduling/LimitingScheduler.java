/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

import org.neo4j.logging.Log;
import org.neo4j.scheduler.CancelListener;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class LimitingScheduler
{
    private final JobScheduler executor;
    private final Group group;
    private final JobsQueue<Runnable> jobsQueue;
    private final Log log;

    private volatile boolean stopped; // implicitly false (initialise in stopped state)
    private volatile ReschedulingJob job;
    private volatile JobHandle<?> jobHandle;

    /**
     * Schedule {@link ReschedulingJob} on the provided {@link JobScheduler}.
     * <p>
     * All calls to the provided {@link JobsQueue} is performed under a lock so it is not required for {@link JobsQueue} to be thread-safe.
     * <p>
     * After a job completes it will schedule a new job from the queue if present.
     *
     * @param jobsQueue When a job completes it will schedule a new job from this queue. This queue should not be blocking.
     */
    public LimitingScheduler( JobScheduler executor, Group group, Log log, JobsQueue<Runnable> jobsQueue )
    {
        this.executor = executor;
        this.group = group;
        this.jobsQueue = jobsQueue;
        this.log = log;
    }

    /**
     * Offers a job to the queue.
     *
     * @throws IllegalStateException if the scheduler is disabled.
     */
    public void offerJob( Runnable runnable )
    {
        // exit without taking a lock if we're already disabled
        throwIfStopped();
        synchronized ( this )
        {
            throwIfStopped();
            jobsQueue.offer( runnable );
            trySchedule();
        }
    }

    private void throwIfStopped()
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Not allowing jobs to be scheduled when disabled" );
        }
    }

    private synchronized void trySchedule()
    {
        if ( job != null )
        {
            return;
        }

        var nextJob = jobsQueue.poll();
        if ( nextJob == null )
        {
            return;
        }

        var currentJob = new ReschedulingJob( nextJob );
        this.job = currentJob;
        this.jobHandle = executor.schedule( group, currentJob );
    }

    /**
     * Prevents adding new jobs and waits for currently running and queued jobs to complete.
     */
    public void stopAndFlush()
    {
        // use synchronized setStopped method so that we know that all in progress offerJob() calls have completed before we disable.
        // That allows us to be sure that no items are going to join the queue after this point
        setStopped();

        try
        {
            flush();
        }
        finally
        {
            abortJob();
            waitTermination( jobHandle );
        }
    }

    /**
     * Synchronized call to jobsQueue.isEmpty because jobsQueue implementation is not thread safe on its own
     */
    private synchronized boolean isEmpty()
    {
        if ( !stopped )
        {
            throw new IllegalStateException( "Checking if the queue is empty before stopping the scheduler is not supported" );
        }
        return jobsQueue.isEmpty();
    }

    /**
     * Waits until all jobs are complete. Does not block concurrent addition of jobs!
     */
    private void flush()
    {
        while ( !isEmpty() )
        {
            waitTermination( jobHandle );
            // the queue should be self-scheduling but we do this anyway to keep moving and to avoid being a busy-loop
            trySchedule();
        }
        // Wait on the last scheduled job to finish
        waitTermination( jobHandle );
    }

    private synchronized void setStopped()
    {
        stopped = true;
    }

    private synchronized void abortJob()
    {
        if ( !isEmpty() )
        {
            log.warn( "LimitingScheduler aborted with unfinished jobs still in its queue" );
        }
        jobsQueue.clear();
        var currentJob = this.job;
        if ( currentJob != null )
        {
            currentJob.abort();
        }
    }

    private class ReschedulingJob implements Runnable, CancelListener
    {
        private final Runnable runnable;
        private volatile boolean aborted;

        private ReschedulingJob( Runnable runnable )
        {
            this.runnable = runnable;
        }

        @Override
        public void run()
        {
            try
            {
                if ( !aborted )
                {
                    runnable.run();
                }
            }
            finally
            {
                job = null;
                // It's not necessary to check if the scheduler is stopped here since we are expected to always flush our whole queue
                trySchedule();
            }
        }

        void abort()
        {
            aborted = true;
        }

        /**
         * Should never be cancelled. But if it does happen, we ensure to remove from registry.
         */
        @Override
        public void cancelled()
        {
            abort();
            job = null;
        }
    }

    private void waitTermination( JobHandle<?> jobHandle )
    {
        if ( jobHandle == null )
        {
            return;
        }

        try
        {
            jobHandle.waitTermination();
        }
        catch ( Exception e )
        {
            job = null;
            log.warn( "Unexpected exception", e );
        }
    }
}
