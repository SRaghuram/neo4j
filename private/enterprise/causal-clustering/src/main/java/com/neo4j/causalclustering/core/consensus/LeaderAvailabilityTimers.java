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
import java.time.Duration;

import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.uniformRandomTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class LeaderAvailabilityTimers
{
    private final long electionIntervalMillis;
    private final long heartbeatIntervalMillis;
    private final Clock clock;
    private final TimerService timerService;
    private final Log log;

    private volatile long lastElectionRenewalMillis;

    private volatile Timer heartbeatTimer;
    private volatile Timer electionTimer;

    LeaderAvailabilityTimers( Duration electionTimeout, Duration heartbeatInterval, Clock clock, TimerService timerService,
            LogProvider logProvider )
    {
        this.electionIntervalMillis = electionTimeout.toMillis();
        this.heartbeatIntervalMillis = heartbeatInterval.toMillis();
        this.clock = clock;
        this.timerService = timerService;
        this.log = logProvider.getLog( getClass() );

        if ( this.electionIntervalMillis < heartbeatIntervalMillis )
        {
            throw new IllegalArgumentException(
                    String.format( "Election timeout %s should not be shorter than heartbeat interval %s", this.electionIntervalMillis, heartbeatIntervalMillis
            ) );
        }
    }

    void start( ThrowingAction<Exception> electionAction, ThrowingAction<Exception> heartbeatAction )
    {
        this.electionTimer = timerService.create( RaftMachine.Timeouts.ELECTION, Group.RAFT_TIMER, renewing( electionAction ) );
        this.electionTimer.set( uniformRandomTimeout( electionIntervalMillis, electionIntervalMillis * 2, MILLISECONDS ) );

        this.heartbeatTimer = timerService.create( RaftMachine.Timeouts.HEARTBEAT, Group.RAFT_TIMER, renewing( heartbeatAction ) );
        this.heartbeatTimer.set( fixedTimeout( heartbeatIntervalMillis, MILLISECONDS ) );

        lastElectionRenewalMillis = clock.millis();
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

    void renewElection()
    {
        lastElectionRenewalMillis = clock.millis();
        if ( electionTimer != null )
        {
            electionTimer.reset();
        }
    }

    boolean isElectionTimedOut()
    {
        return clock.millis() - lastElectionRenewalMillis >= electionIntervalMillis;
    }

    // Getters for immutable values
    long getElectionTimeoutMillis()
    {
        return electionIntervalMillis;
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
