/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RaftMessageMonitoringHandlerTest
{
    private Instant now = Instant.now();
    private Monitors monitors = new Monitors();
    private RaftMessageProcessingMonitor monitor = mock( RaftMessageProcessingMonitor.class );
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> downstream = mock( LifecycleMessageHandler.class );

    private Duration messageQueueDelay = Duration.ofMillis( 5 );
    private Duration messageProcessingDelay = Duration.ofMillis( 7 );
    private RaftMessages.InboundRaftMessageContainer<?> message = RaftMessages.InboundRaftMessageContainer.of(
            now.minus( messageQueueDelay ), IdFactory.randomRaftId(), new RaftMessages.Heartbeat( IdFactory.randomMemberId(), 0, 0, 0 )
    );
    private Clock clock = Clocks.tickOnAccessClock( now, messageProcessingDelay );

    private RaftMessageMonitoringHandler handler = new RaftMessageMonitoringHandler( downstream, clock, monitors );

    @BeforeEach
    void setUp()
    {
        monitors.addMonitorListener( monitor );
    }

    @Test
    void shouldSendMessagesToDelegate()
    {
        // when
        handler.handle( message );

        // then
        verify( downstream ).handle( message );
    }

    @Test
    void shouldUpdateDelayMonitor()
    {
        // when
        handler.handle( message );

        // then
        verify( monitor ).setDelay( messageQueueDelay );
    }

    @Test
    void shouldTimeDelegate()
    {
        // when
        handler.handle( message );

        // then
        verify( monitor ).updateTimer( RaftMessages.Type.HEARTBEAT, messageProcessingDelay );
    }

    @Test
    void shouldDelegateStart() throws Throwable
    {
        // given
        RaftId raftId = IdFactory.randomRaftId();

        // when
        handler.start( raftId );

        // then
        Mockito.verify( downstream ).start( raftId );
    }

    @Test
    void shouldDelegateStop() throws Throwable
    {
        // when
        handler.stop();

        // then
        Mockito.verify( downstream ).stop();
    }
}
