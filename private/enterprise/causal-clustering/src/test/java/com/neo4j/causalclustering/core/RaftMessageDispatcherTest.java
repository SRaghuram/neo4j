/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantRaftIdAwareMessage;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
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
    private final MessageHandler<ReceivedInstantRaftIdAwareMessage<?>> handler = newHandlerMock();
    private final MessageHandler<ReceivedInstantRaftIdAwareMessage<?>> otherHandler = newHandlerMock();

    @Test
    void shouldDispatchToCorrectHandler()
    {
        RaftId id1 = newId();
        ReceivedInstantRaftIdAwareMessage<RaftMessage> message1 = newMessage( id1 );

        RaftId id2 = newId();
        ReceivedInstantRaftIdAwareMessage<RaftMessage> message2 = newMessage( id2 );

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
        RaftId knownId = newId();
        RaftId unknownId = newId();
        dispatcher.registerHandlerChain( knownId, handler );
        ReceivedInstantRaftIdAwareMessage<RaftMessage> message = newMessage( unknownId );

        dispatcher.handle( message );

        verify( handler, never() ).handle( message );
    }

    @Test
    void shouldNotDispatchAfterHandlerDeregistered()
    {
        RaftId id = newId();
        dispatcher.registerHandlerChain( id, handler );
        ReceivedInstantRaftIdAwareMessage<RaftMessage> message = newMessage( id );

        dispatcher.deregisterHandlerChain( id );
        dispatcher.handle( message );

        verify( handler, never() ).handle( message );
    }

    @SuppressWarnings( "unchecked" )
    private static MessageHandler<ReceivedInstantRaftIdAwareMessage<?>> newHandlerMock()
    {
        return mock( MessageHandler.class );
    }

    private static RaftId newId()
    {
        return RaftIdFactory.random();
    }

    private static ReceivedInstantRaftIdAwareMessage<RaftMessage> newMessage( RaftId id )
    {
        return ReceivedInstantRaftIdAwareMessage.of( Instant.now(), id, mock( RaftMessage.class ) );
    }
}
