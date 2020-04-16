/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger stopCount = new AtomicInteger();
    private final Log log;

    private volatile AbortableJob job;
    private volatile JobHandle<?> jobHandle;

    /**
     * Schedule {@link AbortableJob} on the provided {@link JobScheduler}.
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
     * If the job cannot be scheduled right away it will be offered to the jobsQueue. If {@link #abort()} is currently being called,
     * then no jobs are accepted and all jobs currently in the queue will be removed.
     */
    public synchronized void offerJob( Runnable runnable )
    {
        if ( stopCount.get() > 0 )
        {
            return;
        }
        jobsQueue.offer( runnable );
        trySchedule();
    }

    private void trySchedule()
    {
        if ( stopCount.get() > 0 )
        {
            return;
        }
        synchronized ( this )
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

            this.job = new AbortableJob( nextJob );
            this.jobHandle = executor.schedule( group, job );
        }
    }

    /**
     * TODO: Remake this to a stop() and make sure no jobs will be processed afterwards?
     * Aborts any offered jobs that are not running and waits for currently running jobs to complete.
     */
    public void abort()
    {
        // TODO: Why this reference counting thing?
        stopCount.incrementAndGet();
        try
        {
            jobsQueue.clear(); // TODO: Should this also be under synchronized?
            job.abort();
            waitTermination( jobHandle );
        }
        finally
        {
            stopCount.decrementAndGet();
        }
    }

    private class AbortableJob implements Runnable, CancelListener
    {
        private Runnable runnable;
        private volatile boolean aborted;

        private AbortableJob( Runnable runnable )
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
