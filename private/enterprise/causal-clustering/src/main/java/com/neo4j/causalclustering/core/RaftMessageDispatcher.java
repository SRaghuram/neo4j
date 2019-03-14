/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.CopyOnWriteHashMap;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.CappedLogger;

public class RaftMessageDispatcher implements MessageHandler<ReceivedInstantClusterIdAwareMessage<?>>
{
    private final Map<ClusterId,MessageHandler<ReceivedInstantClusterIdAwareMessage<?>>> handlersById = new CopyOnWriteHashMap<>();
    private final CappedLogger log;

    public RaftMessageDispatcher( LogProvider logProvider, Clock clock )
    {
        this.log = createCappedLogger( logProvider, clock );
    }

    @Override
    public void handle( ReceivedInstantClusterIdAwareMessage<?> message )
    {
        ClusterId id = message.clusterId();
        MessageHandler<ReceivedInstantClusterIdAwareMessage<?>> head = handlersById.get( id );
        if ( head == null )
        {
            log.warn( "Unable to process message " + message + " because handler for Raft ID " + id + " is not installed" );
        }
        else
        {
            head.handle( message );
        }
    }

    void registerHandlerChain( ClusterId id, MessageHandler<ReceivedInstantClusterIdAwareMessage<?>> head )
    {
        MessageHandler<ReceivedInstantClusterIdAwareMessage<?>> existingHead = handlersById.putIfAbsent( id, head );
        if ( existingHead != null )
        {
            throw new IllegalArgumentException( "Handler chain for cluster ID " + id + " is already registered" );
        }
    }

    void deregisterHandlerChain( ClusterId id )
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
