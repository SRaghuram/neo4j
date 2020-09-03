/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.CappedLogger;

public class RaftMessageDispatcher implements MessageHandler<InboundRaftMessageContainer<?>>
{
    private final Map<RaftGroupId,MessageHandler<InboundRaftMessageContainer<?>>> handlersById = new ConcurrentHashMap<>();
    private final CappedLogger log;

    RaftMessageDispatcher( LogProvider logProvider, Clock clock )
    {
        this.log = createCappedLogger( logProvider, clock );
    }

    @Override
    public void handle( InboundRaftMessageContainer<?> message )
    {
        RaftGroupId id = message.raftGroupId();
        MessageHandler<InboundRaftMessageContainer<?>> head = handlersById.get( id );
        if ( head == null )
        {
            log.warn( "Unable to process message %s because handler for Raft ID %s is not installed", message, id );
        }
        else
        {
            head.handle( message );
        }
    }

    void registerHandlerChain( RaftGroupId id, MessageHandler<InboundRaftMessageContainer<?>> head )
    {
        MessageHandler<InboundRaftMessageContainer<?>> existingHead = handlersById.putIfAbsent( id, head );
        if ( existingHead != null )
        {
            throw new IllegalArgumentException( "Handler chain for raft ID " + id + " is already registered" );
        }
    }

    void deregisterHandlerChain( RaftGroupId id )
    {
        handlersById.remove( id );
    }

    private CappedLogger createCappedLogger( LogProvider logProvider, Clock clock )
    {
        return new CappedLogger( logProvider.getLog( getClass() ), 5, TimeUnit.SECONDS, clock );
    }
}
