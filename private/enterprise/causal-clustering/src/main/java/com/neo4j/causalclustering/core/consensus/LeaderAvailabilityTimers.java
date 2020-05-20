/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.schedule.TimeoutHandler;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;

import java.time.Clock;

import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.multiTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.uniformRandomTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class LeaderAvailabilityTimers
{

    private final RaftTimersConfig raftTimersConfig;
    private final Clock clock;
    private final TimerService timerService;
    private final Log log;

    private volatile long lastElectionRenewalMillis;

    private volatile Timer heartbeatTimer;
    private volatile Timer electionTimer;

    public volatile ElectionTimerMode electionTimerMode;

    LeaderAvailabilityTimers( RaftTimersConfig raftTimersConfig, Clock clock, TimerService timerService, LogProvider logProvider )
    {
        this.raftTimersConfig = raftTimersConfig;
        this.clock = clock;
        this.timerService = timerService;
        this.log = logProvider.getLog( getClass() );
        this.electionTimerMode = ElectionTimerMode.IMMEDIATE;
    }

    void start( ThrowingAction<Exception> electionAction, ThrowingAction<Exception> heartbeatAction )
    {
        var immediateTimeout = uniformRandomTimeout( 0, raftTimersConfig.detectionDeltaInMillis(), MILLISECONDS );
        var resolutionTimeout = uniformRandomTimeout( raftTimersConfig.resolutionWindowMinInMillis(),
                raftTimersConfig.resolutionWindowMaxInMillis(), MILLISECONDS );
        var detectionTimeout = uniformRandomTimeout( raftTimersConfig.detectionWindowMinInMillis(),
                raftTimersConfig.detectionWindowMaxInMillis(), MILLISECONDS );
        var electionTimeout = multiTimeout( this::getElectionType )
                .addTimeout( ElectionTimerMode.IMMEDIATE, immediateTimeout )
                .addTimeout( ElectionTimerMode.ACTIVE_ELECTION, resolutionTimeout )
                .addTimeout( ElectionTimerMode.FAILURE_DETECTION, detectionTimeout );

        this.electionTimer = timerService.create( RaftMachine.Timeouts.ELECTION, Group.RAFT_HANDLER, renewing( electionAction ) );
        this.electionTimer.set( electionTimeout );

        this.heartbeatTimer = timerService.create( RaftMachine.Timeouts.HEARTBEAT, Group.RAFT_HANDLER, renewing( heartbeatAction ) );
        this.heartbeatTimer.set( fixedTimeout( raftTimersConfig.heartbeatIntervalInMillis(), MILLISECONDS ) );

        lastElectionRenewalMillis = clock.millis();
    }

    private ElectionTimerMode getElectionType()
    {
        return electionTimerMode;
    }

    void stop()
    {
        if ( electionTimer != null )
        {
            electionTimer.kill( SYNC_WAIT );
        }
        if ( heartbeatTimer != null )
        {
            heartbeatTimer.kill( SYNC_WAIT );
        }
    }

    void renewElectionTimer( ElectionTimerMode electionTimerMode )
    {
        lastElectionRenewalMillis = clock.millis();
        this.electionTimerMode = electionTimerMode;
        if ( electionTimer != null )
        {
            electionTimer.reset();
        }
    }

    boolean isElectionTimedOut()
    {
        switch ( electionTimerMode )
        {
        case IMMEDIATE:
            return true;
        case ACTIVE_ELECTION:
            return clock.millis() - lastElectionRenewalMillis >= raftTimersConfig.resolutionWindowMinInMillis();
        case FAILURE_DETECTION:
            return clock.millis() - lastElectionRenewalMillis >= raftTimersConfig.detectionWindowMinInMillis();
        default:
            throw new IllegalArgumentException( "Unknown timeout type: " + electionTimerMode );
        }
    }

    private TimeoutHandler renewing( ThrowingAction<Exception> action )
    {
        return timeout ->
        {
            try
            {
                action.apply();
            }
            catch ( Exception e )
            {
                log.error( "Failed to process timeout.", e );
            }
            timeout.reset();
        };
    }
}
