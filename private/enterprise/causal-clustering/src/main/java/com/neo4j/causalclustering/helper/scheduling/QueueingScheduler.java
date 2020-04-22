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

public class QueueingScheduler
{
    private final JobScheduler executor;
    private final Group group;
    private final JobsQueue<Runnable> jobsQueue;
    private final Log log;

    private volatile boolean enabled = true;
    private volatile ReschedulingJob job;
    private volatile JobHandle<?> jobHandle;

    /**
     * Schedule {@link ReschedulingJob} on the provided {@link JobScheduler}.
     *
     * All calls to the provided {@link JobsQueue} is performed under a lock so it is not
     * required for {@link JobsQueue} to be thread-safe.
     *
     * After a job completes it will schedule a new job from the queue if present.
     *
     * @param jobsQueue        When a job completes it will schedule a new job from this queue. This queue should not be blocking.
     */
    public QueueingScheduler( JobScheduler executor, Group group, Log log, JobsQueue<Runnable> jobsQueue )
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
    public synchronized void offerJob( Runnable runnable )
    {
        if ( !enabled )
        {
            throw new IllegalStateException( "Not allowing jobs to be scheduled when disabled" );
        }
        jobsQueue.offer( runnable );
        trySchedule();
    }

    private synchronized void trySchedule()
    {
        if ( !enabled )
        {
            return;
        }
        if ( job != null )
        {
            return;
        }

        var nextJob = jobsQueue.poll();
        if ( nextJob == null )
        {
            return;
        }

        this.job = new ReschedulingJob( nextJob );
        this.jobHandle = executor.schedule( group, job );
    }

    /**
     * Aborts any offered jobs that are not running and waits for currently running jobs to complete.
     */
    public void disable()
    {
        enabled = false;
        abortJob();
        waitTermination( jobHandle );
    }

    public void enable()
    {
        enabled = true;
    }

    private synchronized void abortJob()
    {
        jobsQueue.clear();
        if ( job != null )
        {
            job.abort();
        }
    }

    private class ReschedulingJob implements Runnable, CancelListener
    {
        private Runnable runnable;
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
                trySchedule();
            }
        }

        public void abort()
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
