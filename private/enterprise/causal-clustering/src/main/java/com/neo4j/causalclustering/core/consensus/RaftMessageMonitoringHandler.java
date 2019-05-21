/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.neo4j.monitoring.Monitors;

public class RaftMessageMonitoringHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> raftMessageHandler;
    private final Clock clock;
    private final RaftMessageProcessingMonitor raftMessageDelayMonitor;

    public RaftMessageMonitoringHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> raftMessageHandler,
            Clock clock, Monitors monitors )
    {
        this.raftMessageHandler = raftMessageHandler;
        this.clock = clock;
        this.raftMessageDelayMonitor = monitors.newMonitor( RaftMessageProcessingMonitor.class );
    }

    public static ComposableMessageHandler composable( Clock clock, Monitors monitors )
    {
        return delegate -> new RaftMessageMonitoringHandler( delegate, clock, monitors );
    }

    @Override
    public synchronized void handle( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> incomingMessage )
    {
        Instant start = clock.instant();

        logDelay( incomingMessage, start );

        timeHandle( incomingMessage, start );
    }

    private void timeHandle( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> incomingMessage, Instant start )
    {
        try
        {
            raftMessageHandler.handle( incomingMessage );
        }
        finally
        {
            Duration duration = Duration.between( start, clock.instant() );
            raftMessageDelayMonitor.updateTimer( incomingMessage.type(), duration );
        }
    }

    private void logDelay( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> incomingMessage, Instant start )
    {
        Duration delay = Duration.between( incomingMessage.receivedAt(), start );

        raftMessageDelayMonitor.setDelay( delay );
    }

    @Override
    public void start( RaftId raftId ) throws Exception
    {
        raftMessageHandler.start( raftId );
    }

    @Override
    public void stop() throws Exception
    {
        raftMessageHandler.stop();
    }
}
