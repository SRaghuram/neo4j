/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;

public class CatchupJobScheduler extends LifecycleAdapter
{

    public enum Timers implements TimerService.TimerName
    {
        TX_PULLER_TIMER
    }

    private final TimerService timerService;
    private final CatchupJob catchupJob;
    private final Duration catchupJobInterval;
    private Timer timer;

    public CatchupJobScheduler( TimerService timerService, CatchupJob catchupJob, Duration catchupJobInterval )
    {
        this.timerService = timerService;
        this.catchupJob = catchupJob;
        this.catchupJobInterval = catchupJobInterval;
    }

    @Override
    public void start() throws Exception
    {
        timer = timerService.create( Timers.TX_PULLER_TIMER, Group.PULL_UPDATES, timeout ->
        {
            catchupJob.execute();
            if ( catchupJob.canSchedule() )
            {
                timer.reset();
            }
        } );
        timer.set( fixedTimeout( catchupJobInterval.toMillis(), TimeUnit.MILLISECONDS ) );
    }

    @Override
    public void stop() throws Exception
    {
        if ( timer != null )
        {
            timer.kill( Timer.CancelMode.SYNC_WAIT );
        }
    }
}
