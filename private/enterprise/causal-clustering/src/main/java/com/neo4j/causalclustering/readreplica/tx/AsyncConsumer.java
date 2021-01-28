/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import java.util.concurrent.BlockingQueue;

import org.neo4j.logging.Log;

class AsyncConsumer implements Runnable
{
    private final BlockingQueue<Runnable> jobs;
    private final Log log;
    private volatile boolean stopPolling;

    AsyncConsumer( BlockingQueue<Runnable> jobs, Log log )
    {
        this.jobs = jobs;
        this.log = log;
    }

    @Override
    public void run()
    {
        while ( !stopPolling )
        {
            Runnable job;
            try
            {
                job = jobs.take();
            }
            catch ( InterruptedException e )
            {
                log.warn( "Unexpected interrupt", e);
                continue;
            }
            runJob( job );
        }
        log.info( "Stopping. Completing outstanding jobs" );
        jobs.forEach( this::runJob );
        log.info( "Jobs completed" );
    }

    void stopPolling()
    {
        stopPolling = true;
        // add empty job to progress take()
        jobs.add( () -> {} );
    }

    private void runJob( Runnable job )
    {
        try
        {
            job.run();
        }
        catch ( Throwable t )
        {
            log.error( "Unexpected error", t );
        }
    }
}
