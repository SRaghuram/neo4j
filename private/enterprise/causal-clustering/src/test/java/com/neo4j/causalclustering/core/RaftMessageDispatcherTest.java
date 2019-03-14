/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RaftMessageDispatcherTest
{
    private final RaftMessageDispatcher dispatcher = new RaftMessageDispatcher( NullLogProvider.getInstance(), Clock.systemUTC() );
    private final MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler = newHandlerMock();
    private final MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> otherHandler = newHandlerMock();

    @Test
    void shouldDispatchToCorrectHandler()
    {
        ClusterId id1 = newId();
        ReceivedInstantClusterIdAwareMessage<RaftMessage> message1 = newMessage( id1 );

        ClusterId id2 = newId();
        ReceivedInstantClusterIdAwareMessage<RaftMessage> message2 = newMessage( id2 );

        dispatcher.registerHandlerChain( id1, handler );
        dispatcher.registerHandlerChain( id2, otherHandler );

        dispatcher.handle( message1 );
        dispatcher.handle( message2 );

        verify( handler ).handle( message1 );
        verify( handler, never() ).handle( message2 );
        verify( otherHandler, never() ).handle( message1 );
        verify( otherHandler ).handle( message2 );
    }

    @Test
    void shouldNotDispatchWhenHandlerNotRegistered()
    {
        ClusterId knownId = newId();
        ClusterId unknownId = newId();
        dispatcher.registerHandlerChain( knownId, handler );
        ReceivedInstantClusterIdAwareMessage<RaftMessage> message = newMessage( unknownId );

        dispatcher.handle( message );

        verify( handler, never() ).handle( message );
    }

    @Test
    void shouldNotDispatchAfterHandlerDeregistered()
    {
        ClusterId id = newId();
        dispatcher.registerHandlerChain( id, handler );
        ReceivedInstantClusterIdAwareMessage<RaftMessage> message = newMessage( id );

        dispatcher.deregisterHandlerChain( id );
        dispatcher.handle( message );

        verify( handler, never() ).handle( message );
    }

    @SuppressWarnings( "unchecked" )
    private static MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> newHandlerMock()
    {
        return mock( MessageHandler.class );
    }

    private static ClusterId newId()
    {
        return new ClusterId( UUID.randomUUID() );
    }

    private static ReceivedInstantClusterIdAwareMessage<RaftMessage> newMessage( ClusterId id )
    {
        return ReceivedInstantClusterIdAwareMessage.of( Instant.now(), id, mock( RaftMessage.class ) );
    }
}
