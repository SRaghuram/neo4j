/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

/**
 * This scheduler is part of slave lifecycle that will schedule periodic pulling on slave switch
 * and turn them off during slave shutdown.
 *
 * @see UpdatePuller
 */
public class UpdatePullerScheduler extends LifecycleAdapter
{
    private final JobScheduler scheduler;
    private final Log log;
    private final UpdatePuller updatePuller;
    private final long pullIntervalMillis;
    private JobHandle intervalJobHandle;

    public UpdatePullerScheduler( JobScheduler scheduler, LogProvider logProvider, UpdatePuller updatePullingThread,
            long pullIntervalMillis )
    {
        this.scheduler = scheduler;
        this.log = logProvider.getLog( getClass() );
        this.updatePuller = updatePullingThread;
        this.pullIntervalMillis = pullIntervalMillis;
    }

    @Override
    public void init()
    {
        if ( pullIntervalMillis > 0 )
        {
            intervalJobHandle = scheduler.scheduleRecurring( Group.PULL_UPDATES, () ->
            {
                try
                {
                    updatePuller.pullUpdates();
                }
                catch ( InterruptedException e )
                {
                    log.error( "Pull updates failed", e );
                }
            }, pullIntervalMillis, pullIntervalMillis, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public void shutdown()
    {
        if ( intervalJobHandle != null )
        {
            intervalJobHandle.cancel( false );
        }
    }
}
