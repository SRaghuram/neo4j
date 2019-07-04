/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;

import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

class ClusterBindingHandlerTest
{
    private RaftId raftId = RaftIdFactory.random();

    private RaftMessages.ReceivedInstantRaftIdAwareMessage<?> heartbeat =
            RaftMessages.ReceivedInstantRaftIdAwareMessage.of( Instant.now(), raftId,
                    new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 0L, 0, 0 ) );

    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> delegate = Mockito.mock( LifecycleMessageHandler.class );

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
        handler.start( raftId );
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
        handler.start( raftId );

        // when
        handler.handle( RaftMessages.ReceivedInstantRaftIdAwareMessage.of(
                Instant.now(), RaftIdFactory.random(),
                new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 0L, 0, 0 )
        ) );

        // then
        verify( delegate, Mockito.never() ).handle( ArgumentMatchers.any( RaftMessages.ReceivedInstantRaftIdAwareMessage.class ) );
    }

    @Test
    void shouldDelegateMessages() throws Throwable
    {
        // given
        handler.start( raftId );

        // when
        handler.handle( heartbeat );

        // then
        verify( delegate ).handle( heartbeat );
    }

    @Test
    void shouldDelegateStartCalls() throws Throwable
    {
        // when
        handler.start( raftId );

        // then
        verify( delegate ).start( raftId );
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
        handler.start( raftId );

        // then
        verify( messageDispatcher ).registerHandlerChain( raftId, handler );
    }

    @Test
    void shouldDeregisterInRaftMessageDispatcherWhenStopped() throws Throwable
    {
        // given
        handler.start( raftId );

        // when
        handler.stop();

        // then
        InOrder inOrder = inOrder( messageDispatcher );
        inOrder.verify( messageDispatcher ).registerHandlerChain( raftId, handler );
        inOrder.verify( messageDispatcher ).deregisterHandlerChain( raftId );
    }

    @Test
    void shouldDeregisterInRaftMessageDispatcherWhenDelegateFailsToStop() throws Throwable
    {
        // given
        RuntimeException error = new RuntimeException( "Unable to stop" );
        Mockito.doThrow( error ).when( delegate ).stop();
        handler.start( raftId );

        // when
        RuntimeException thrownError = assertThrows( RuntimeException.class, handler::stop );
        assertEquals( error, thrownError );

        // then
        InOrder inOrder = inOrder( messageDispatcher, delegate );
        inOrder.verify( delegate ).start( raftId );
        inOrder.verify( messageDispatcher ).registerHandlerChain( raftId, handler );
        inOrder.verify( delegate ).stop();
        inOrder.verify( messageDispatcher ).deregisterHandlerChain( raftId );
    }
}
