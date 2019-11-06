/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantRaftIdAwareMessage;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.CappedLogger;

public class RaftMessageDispatcher implements MessageHandler<ReceivedInstantRaftIdAwareMessage<?>>
{
    private final Map<RaftId,MessageHandler<ReceivedInstantRaftIdAwareMessage<?>>> handlersById = new ConcurrentHashMap<>();
    private final CappedLogger log;

    RaftMessageDispatcher( LogProvider logProvider, Clock clock )
    {
        this.log = createCappedLogger( logProvider, clock );
    }

    @Override
    public void handle( ReceivedInstantRaftIdAwareMessage<?> message )
    {
        RaftId id = message.raftId();
        MessageHandler<ReceivedInstantRaftIdAwareMessage<?>> head = handlersById.get( id );
        if ( head == null )
        {
            log.warn( "Unable to process message %s because handler for Raft ID %s is not installed", message, id );
        }
        else
        {
            head.handle( message );
        }
    }

    void registerHandlerChain( RaftId id, MessageHandler<ReceivedInstantRaftIdAwareMessage<?>> head )
    {
        MessageHandler<ReceivedInstantRaftIdAwareMessage<?>> existingHead = handlersById.putIfAbsent( id, head );
        if ( existingHead != null )
        {
            throw new IllegalArgumentException( "Handler chain for raft ID " + id + " is already registered" );
        }
    }

    void deregisterHandlerChain( RaftId id )
    {
        handlersById.remove( id );
    }

    private CappedLogger createCappedLogger( LogProvider logProvider, Clock clock )
    {
        CappedLogger logger = new CappedLogger( logProvider.getLog( getClass() ) );
        logger.setTimeLimit( 5, TimeUnit.SECONDS, clock );
        return logger;
    }
}
