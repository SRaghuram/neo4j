/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;

import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

class ClusterBindingHandlerTest
{
    private RaftGroupId raftGroupId = IdFactory.randomRaftId();

    private RaftMessages.InboundRaftMessageContainer<?> heartbeat =
            RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftGroupId,
                                                         new RaftMessages.Heartbeat( IdFactory.randomRaftMemberId(), 0L, 0, 0 ) );

    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> delegate = Mockito.mock( LifecycleMessageHandler.class );

    private RaftMessageDispatcher messageDispatcher = Mockito.mock( RaftMessageDispatcher.class );
    private ClusterBindingHandler handler = new ClusterBindingHandler( messageDispatcher, delegate, NullLogProvider.getInstance() );

    @Test
    void shouldDropMessagesIfHasNotBeenStarted()
    {
        // when
        handler.handle( heartbeat );

        // then
        verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    void shouldDropMessagesIfHasBeenStopped() throws Throwable
    {
        // given
        handler.start( raftGroupId );
        handler.stop();

        // when
        handler.handle( heartbeat );

        // then
        verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    void shouldDropMessagesIfForDifferentRaftId() throws Throwable
    {
        // given
        handler.start( raftGroupId );

        // when
        handler.handle( RaftMessages.InboundRaftMessageContainer.of(
                Instant.now(), IdFactory.randomRaftId(),
                new RaftMessages.Heartbeat( IdFactory.randomRaftMemberId(), 0L, 0, 0 )
        ) );

        // then
        verify( delegate, Mockito.never() ).handle( any( RaftMessages.InboundRaftMessageContainer.class ) );
    }

    @Test
    void shouldDelegateMessages() throws Throwable
    {
        // given
        handler.start( raftGroupId );

        // when
        handler.handle( heartbeat );

        // then
        verify( delegate ).handle( heartbeat );
    }

    @Test
    void shouldDelegateStartCalls() throws Throwable
    {
        // when
        handler.start( raftGroupId );

        // then
        verify( delegate ).start( raftGroupId );
    }

    @Test
    void shouldDelegateStopCalls() throws Throwable
    {
        // when
        handler.stop();

        // then
        verify( delegate ).stop();
    }

    @Test
    void shouldRegisterInRaftMessageDispatcherWhenStarted() throws Throwable
    {
        // when
        handler.start( raftGroupId );

        // then
        verify( messageDispatcher ).registerHandlerChain( raftGroupId, handler );
    }

    @Test
    void shouldDeregisterInRaftMessageDispatcherWhenStopped() throws Throwable
    {
        // given
        handler.start( raftGroupId );

        // when
        handler.stop();

        // then
        InOrder inOrder = inOrder( messageDispatcher );
        inOrder.verify( messageDispatcher ).registerHandlerChain( raftGroupId, handler );
        inOrder.verify( messageDispatcher ).deregisterHandlerChain( raftGroupId );
    }

    @Test
    void shouldDeregisterInRaftMessageDispatcherWhenDelegateFailsToStop() throws Throwable
    {
        // given
        RuntimeException error = new RuntimeException( "Unable to stop" );
        Mockito.doThrow( error ).when( delegate ).stop();
        handler.start( raftGroupId );

        // when
        RuntimeException thrownError = assertThrows( RuntimeException.class, handler::stop );
        assertEquals( error, thrownError );

        // then
        InOrder inOrder = inOrder( messageDispatcher, delegate );
        inOrder.verify( delegate ).start( raftGroupId );
        inOrder.verify( messageDispatcher ).registerHandlerChain( raftGroupId, handler );
        inOrder.verify( messageDispatcher ).deregisterHandlerChain( raftGroupId );
        inOrder.verify( delegate ).stop();
    }

    @Test
    void shouldStopWhenNotStarted()
    {
        var logProvider = NullLogProvider.getInstance();
        handler = new ClusterBindingHandler( new RaftMessageDispatcher( logProvider, Clock.systemUTC() ), delegate, logProvider );

        assertDoesNotThrow( handler::stop );
    }

    @Test
    void shouldStopDelegateIfMessageDispatcherThrows() throws Exception
    {
        var error = new RuntimeException();
        doThrow( error ).when( messageDispatcher ).deregisterHandlerChain( raftGroupId );
        handler.start( raftGroupId );

        var thrownError = assertThrows( RuntimeException.class, handler::stop );

        assertEquals( error, thrownError );
        var inOrder = inOrder( messageDispatcher, delegate );
        inOrder.verify( messageDispatcher ).deregisterHandlerChain( raftGroupId );
        inOrder.verify( delegate ).stop();
    }
}
