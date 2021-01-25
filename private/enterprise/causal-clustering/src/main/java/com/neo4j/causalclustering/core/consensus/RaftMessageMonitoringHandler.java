/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.neo4j.monitoring.Monitors;

public class RaftMessageMonitoringHandler implements LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
{
    private final LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> raftMessageHandler;
    private final Clock clock;
    private final RaftMessageProcessingMonitor raftMessageDelayMonitor;

    public RaftMessageMonitoringHandler( LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> raftMessageHandler,
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
    public synchronized void handle( RaftMessages.InboundRaftMessageContainer<?> incomingMessage )
    {
        Instant start = clock.instant();

        logDelay( incomingMessage, start );

        timeHandle( incomingMessage, start );
    }

    private void timeHandle( RaftMessages.InboundRaftMessageContainer<?> incomingMessage, Instant start )
    {
        try
        {
            raftMessageHandler.handle( incomingMessage );
        }
        finally
        {
            Duration duration = Duration.between( start, clock.instant() );
            raftMessageDelayMonitor.updateTimer( incomingMessage.message().type(), duration );
        }
    }

    private void logDelay( RaftMessages.InboundRaftMessageContainer<?> incomingMessage, Instant start )
    {
        Duration delay = Duration.between( incomingMessage.receivedAt(), start );

        raftMessageDelayMonitor.setDelay( delay );
    }

    @Override
    public void start( RaftGroupId raftGroupId ) throws Exception
    {
        raftMessageHandler.start( raftGroupId );
    }

    @Override
    public void stop() throws Exception
    {
        raftMessageHandler.stop();
    }
}
