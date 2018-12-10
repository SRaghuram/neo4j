/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.schedule.TimeoutHandler;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.uniformRandomTimeout;
import static com.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.ASYNC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class LeaderAvailabilityTimers
{
    private final long electionTimeout;
    private final long heartbeatInterval;
    private final Clock clock;
    private final TimerService timerService;
    private final Log log;

    private volatile long lastElectionRenewalMillis;

    private Timer heartbeatTimer;
    private Timer electionTimer;

    LeaderAvailabilityTimers( Duration electionTimeout, Duration heartbeatInterval, Clock clock, TimerService timerService,
            LogProvider logProvider )
    {
        this.electionTimeout = electionTimeout.toMillis();
        this.heartbeatInterval = heartbeatInterval.toMillis();
        this.clock = clock;
        this.timerService = timerService;
        this.log = logProvider.getLog( getClass() );

        if ( this.electionTimeout < this.heartbeatInterval )
        {
            throw new IllegalArgumentException( String.format(
                            "Election timeout %s should not be shorter than heartbeat interval %s", this.electionTimeout, this.heartbeatInterval
            ) );
        }
    }

    synchronized void start( ThrowingConsumer<Clock, Exception> electionAction, ThrowingConsumer<Clock, Exception> heartbeatAction )
    {
        this.electionTimer = timerService.create( RaftMachine.Timeouts.ELECTION, Group.RAFT_TIMER, renewing( electionAction) );
        this.electionTimer.set( uniformRandomTimeout( electionTimeout, electionTimeout * 2, MILLISECONDS ) );

        this.heartbeatTimer = timerService.create( RaftMachine.Timeouts.HEARTBEAT, Group.RAFT_TIMER, renewing( heartbeatAction ) );
        this.heartbeatTimer.set( fixedTimeout( heartbeatInterval, MILLISECONDS ) );

        lastElectionRenewalMillis = clock.millis();
    }

    synchronized void stop()
    {
        if ( electionTimer != null )
        {
            electionTimer.cancel( ASYNC );
        }
        if ( heartbeatTimer != null )
        {
            heartbeatTimer.cancel( ASYNC );
        }
    }

    synchronized void renewElection()
    {
        lastElectionRenewalMillis = clock.millis();
        if ( electionTimer != null )
        {
            electionTimer.reset();
        }
    }

    synchronized boolean isElectionTimedOut()
    {
        return clock.millis() - lastElectionRenewalMillis >= electionTimeout;
    }

    // Getters for immutable values
    long getElectionTimeout()
    {
        return electionTimeout;
    }

    private TimeoutHandler renewing( ThrowingConsumer<Clock, Exception> action )
    {
        return timeout ->
        {
            try
            {
                action.accept( clock );
            }
            catch ( Exception e )
            {
                log.error( "Failed to process timeout.", e );
            }
            timeout.reset();
        };
    }
}
