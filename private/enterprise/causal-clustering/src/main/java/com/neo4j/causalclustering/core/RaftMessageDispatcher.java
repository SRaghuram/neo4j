/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftMessageDispatcher implements MessageHandler<ReceivedInstantClusterIdAwareMessage<?>>
{
    private final Map<ClusterId,MessageHandler<ReceivedInstantClusterIdAwareMessage<?>>> handlersById = new ConcurrentHashMap<>();
    private final Log log;

    public RaftMessageDispatcher( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void handle( ReceivedInstantClusterIdAwareMessage<?> message )
    {
        ClusterId id = message.clusterId();
        MessageHandler<ReceivedInstantClusterIdAwareMessage<?>> head = handlersById.get( id );
        if ( head == null )
        {
            log.debug( "Message handling has been stopped, dropping the message: %s", message );
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
}
