/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;

import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RaftMessageDispatcherTest
{
    private final RaftMessageDispatcher dispatcher = new RaftMessageDispatcher( NullLogProvider.getInstance(), Clock.systemUTC() );
    private final MessageHandler<InboundRaftMessageContainer<?>> handler = newHandlerMock();
    private final MessageHandler<InboundRaftMessageContainer<?>> otherHandler = newHandlerMock();

    @Test
    void shouldDispatchToCorrectHandler()
    {
        RaftId id1 = newId();
        InboundRaftMessageContainer<RaftMessage> message1 = newMessage( id1 );

        RaftId id2 = newId();
        InboundRaftMessageContainer<RaftMessage> message2 = newMessage( id2 );

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
        InboundRaftMessageContainer<RaftMessage> message = newMessage( unknownId );

        dispatcher.handle( message );

        verify( handler, never() ).handle( message );
    }

    @Test
    void shouldNotDispatchAfterHandlerDeregistered()
    {
        RaftId id = newId();
        dispatcher.registerHandlerChain( id, handler );
        InboundRaftMessageContainer<RaftMessage> message = newMessage( id );

        dispatcher.deregisterHandlerChain( id );
        dispatcher.handle( message );

        verify( handler, never() ).handle( message );
    }

    @SuppressWarnings( "unchecked" )
    private static MessageHandler<InboundRaftMessageContainer<?>> newHandlerMock()
    {
        return mock( MessageHandler.class );
    }

    private static RaftId newId()
    {
        return RaftIdFactory.random();
    }

    private static InboundRaftMessageContainer<RaftMessage> newMessage( RaftId id )
    {
        return InboundRaftMessageContainer.of( Instant.now(), id, mock( RaftMessage.class ) );
    }
}
