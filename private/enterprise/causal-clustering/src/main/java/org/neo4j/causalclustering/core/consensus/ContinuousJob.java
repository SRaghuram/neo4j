/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.concurrent.ThreadFactory;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Invokes the supplied task continuously when started. The supplied task
 * should be short since the abort flag is checked in between invocations.
 */
public class ContinuousJob extends LifecycleAdapter
{
    private final AbortableJob abortableJob;
    private final Log log;
    private final Thread thread;

    public ContinuousJob( ThreadFactory threadFactory, Runnable task, LogProvider logProvider )
    {
        this.abortableJob = new AbortableJob( task );
        this.thread = threadFactory.newThread( abortableJob );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        abortableJob.keepRunning = true;
        thread.start();
    }

    @Override
    public void stop() throws Throwable
    {
        log.info( "ContinuousJob " + thread.getName() + " stopping" );
        abortableJob.keepRunning = false;
        thread.join();
    }

    private static class AbortableJob implements Runnable
    {
        private final Runnable task;
        private volatile boolean keepRunning;

        AbortableJob( Runnable task )
        {
            this.task = task;
        }

        @Override
        public void run()
        {
            while ( keepRunning )
            {
                task.run();
            }
        }
    }
}
