/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.Log;
import org.neo4j.scheduler.CancelListener;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.util.Preconditions.requirePositive;

public class QueueingScheduler
{
    private final JobScheduler scheduler;
    private Group group;
    private int maxScheduledJobs;
    private final ScheduledJobsTracker scheduledJobs;
    private final JobsQueue<Runnable> queuedJobs;
    private final AtomicInteger stopCount = new AtomicInteger();

    /**
     * Schedule {@link AbortableJob} on the provided {@link JobScheduler} and keeps track of how many currently running jobs are scheduled. It can also abort
     * all not yet started jobs. All calls to the provided {@link JobsQueue} is performed under a lock so it is not required for {@link JobsQueue} to be
     * thread-safe.
     * <p>
     * After a job completes it will schedule a new job from the queue if present.
     *
     * @param maxScheduledJobs defines how many concurrent jobs that can be scheduled at the same time
     * @param jobsQueue        if {@param maxScheduledJobs} is reached then any offered jobs will be put on this queue. When a job completes it will schedule a
     *                         new job from this queue. This queue should not be blocking.
     */
    public QueueingScheduler( JobScheduler scheduler, Group group, Log log, int maxScheduledJobs, JobsQueue<Runnable> jobsQueue )
    {
        requirePositive( maxScheduledJobs );
        this.scheduler = scheduler;
        this.group = group;
        this.maxScheduledJobs = maxScheduledJobs;
        this.queuedJobs = jobsQueue;
        scheduledJobs = new ScheduledJobsTracker( log );
    }

    /**
     * The job that is offer has no guarantee that it will run. If it cannot be scheduled right away it will be offered to the queue which may or may not accept
     * the job. If {@link #stopAll()} is currently being called, then no jobs are accepted and all jobs currently in the queue will be removed.
     */
    public synchronized void offerJob( Runnable runnable )
    {
        if ( stopCount.get() > 0 )
        {
            return;
        }
        queuedJobs.offer( runnable );
        tryNextJob();
    }

    private void tryNextJob()
    {
        if ( stopCount.get() > 0 )
        {
            return;
        }
        synchronized ( this )
        {
            if ( scheduledJobs.scheduledJobs() < maxScheduledJobs )
            {
                var nextJob = queuedJobs.poll();
                if ( nextJob != null )
                {
                    var job = new AbortableJob( nextJob );
                    var handle = scheduler.schedule( group, job );
                    scheduledJobs.put( job, handle );
                }
            }
        }
    }

    public void stopAll()
    {
        stopCount.incrementAndGet();
        try
        {
            abortAll();
            scheduledJobs.awaitAll();
        }
        finally
        {
            stopCount.decrementAndGet();
        }
    }

    synchronized void abortAll()
    {
        queuedJobs.clear();
        scheduledJobs.abortAll();
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
                scheduledJobs.remove( this );
                tryNextJob();
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
            scheduledJobs.remove( this );
        }
    }

    private static class ScheduledJobsTracker
    {
        private final ConcurrentHashMap<AbortableJob,JobHandle<?>> jobs = new ConcurrentHashMap<>();
        private final Log log;
        private int size;

        private ScheduledJobsTracker( Log log )
        {
            this.log = log;
        }

        synchronized void put( AbortableJob job, JobHandle<?> handle )
        {
            size++;
            jobs.put( job, handle );
        }

        synchronized void remove( AbortableJob job )
        {
            size--;
            jobs.remove( job );
        }

        void abortAll()
        {
            jobs.keySet().forEach( AbortableJob::abort );
        }

        void awaitAll()
        {
            jobs.forEach( ( key, value ) ->
                          {
                              try
                              {
                                  value.waitTermination();
                              }
                              catch ( CancellationException e )
                              {
                                  // Cancelled jobs may not have been removed from the registry.
                                  jobs.remove( key );
                                  log.warn( "Job has been unexpectedly cancelled by some other process" );
                              }
                              catch ( InterruptedException e )
                              {
                                  log.warn( "Unexpected interrupt", e );
                              }
                              catch ( ExecutionException e )
                              {
                                  log.warn( "Exception waiting for job to finish", e );
                              }
                          } );
        }

        int scheduledJobs()
        {
            return size;
        }
    }
}
