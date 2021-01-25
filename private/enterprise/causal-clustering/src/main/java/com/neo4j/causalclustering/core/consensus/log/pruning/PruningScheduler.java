/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.pruning;

import com.neo4j.causalclustering.core.state.RaftLogPruner;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PruningScheduler extends LifecycleAdapter
{
    private final RaftLogPruner logPruner;
    private final JobScheduler scheduler;
    private final long recurringPeriodMillis;
    private final Runnable job = new Runnable()
    {
        @Override
        public void run()
        {
            try
            {
                checkPointing = true;
                if ( stopped )
                {
                    return;
                }
                logPruner.prune();
            }
            catch ( IOException e )
            {
                // no need to reschedule since the check pointer has raised a kernel panic and a shutdown is expected
                throw new UnderlyingStorageException( e );
            }
            finally
            {
                checkPointing = false;
            }

            // reschedule only if it is not stopped
            if ( !stopped )
            {
                handle = scheduler.schedule( Group.RAFT_LOG_PRUNING, job, recurringPeriodMillis, MILLISECONDS );
            }
        }
    };
    private final Log log;

    private volatile JobHandle handle;
    private volatile boolean stopped;
    private volatile boolean checkPointing;
    private final BooleanSupplier checkPointingCondition = new BooleanSupplier()
    {
        @Override
        public boolean getAsBoolean()
        {
            return !checkPointing;
        }
    };

    public PruningScheduler( RaftLogPruner logPruner, JobScheduler scheduler, long recurringPeriodMillis, LogProvider
            logProvider )
    {
        this.logPruner = logPruner;
        this.scheduler = scheduler;
        this.recurringPeriodMillis = recurringPeriodMillis;
        log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        handle = scheduler.schedule( Group.RAFT_LOG_PRUNING, job, recurringPeriodMillis, MILLISECONDS );
    }

    @Override
    public void stop()
    {
        log.info( "PruningScheduler stopping" );
        stopped = true;
        if ( handle != null )
        {
            handle.cancel();
        }
        Predicates.awaitForever( checkPointingCondition, 100, MILLISECONDS );
    }
}
