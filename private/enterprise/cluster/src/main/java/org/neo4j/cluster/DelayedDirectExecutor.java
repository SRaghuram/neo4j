/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Executor that executes the Runnables when drain() is called. Allows async jobs to be scheduled, and then
 * run in a synchronous fashion.
 */
public class DelayedDirectExecutor extends AbstractExecutorService
{
    private List<Runnable> runnables = new ArrayList<>();

    private final Log log;

    public DelayedDirectExecutor( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination( long timeout, TimeUnit unit )
    {
        return true;
    }

    @Override
    public synchronized void execute( Runnable command )
    {
        runnables.add( command );
    }

    public void drain()
    {
        List<Runnable> currentRunnables;
        synchronized ( this )
        {
            currentRunnables = runnables;
            runnables = new ArrayList<>();
        }
        for ( Runnable runnable : currentRunnables )
        {
            try
            {
                runnable.run();
            }
            catch ( Throwable t )
            {
                log.error( "Runnable failed", t );
            }
        }
    }
}
