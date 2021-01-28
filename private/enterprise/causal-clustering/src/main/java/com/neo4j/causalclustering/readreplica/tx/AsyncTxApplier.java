/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class AsyncTxApplier extends LifecycleAdapter
{
    private final JobScheduler jobScheduler;
    private final BlockingQueue<Runnable> jobsQueue = new LinkedBlockingQueue<>();
    private final Log log;
    private final AsyncConsumer asyncConsumer;
    private Thread applierThread;
    private volatile boolean stopped;

    public AsyncTxApplier( JobScheduler jobScheduler, LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        this.jobScheduler = jobScheduler;
        asyncConsumer = new AsyncConsumer( jobsQueue, log );
    }

    @Override
    public void start() throws Exception
    {
        log.info( "Starting applier thread" );
        applierThread = jobScheduler.threadFactory( Group.APPLY_UPDATES ).newThread( asyncConsumer );
        applierThread.start();
    }

    @Override
    public void stop() throws Exception
    {
        log.info( "Stopping applier thread" );
        stopped = true;
        asyncConsumer.stopPolling();
        applierThread.join();
    }

    public void add( Runnable applierTask )
    {
        if ( stopped )
        {
            log.warn( "Applier thead has been stopped. No new tasks is accepted" );
            return;
        }
        jobsQueue.add( applierTask );
    }
}
