/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Objects;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterBindingHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> delegateHandler;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final Log log;

    private volatile RaftId boundRaftId;

    public ClusterBindingHandler( RaftMessageDispatcher raftMessageDispatcher,
            LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> delegateHandler,
            LogProvider logProvider )
    {
        this.delegateHandler = delegateHandler;
        this.raftMessageDispatcher = raftMessageDispatcher;
        log = logProvider.getLog( getClass() );
    }

    public static ComposableMessageHandler composable( RaftMessageDispatcher raftMessageDispatcher, LogProvider logProvider )
    {
        return delegate -> new ClusterBindingHandler( raftMessageDispatcher, delegate, logProvider );
    }

    @Override
    public void start( RaftId raftId ) throws Exception
    {
        boundRaftId = raftId;
        delegateHandler.start( raftId );
        raftMessageDispatcher.registerHandlerChain( raftId, this );
    }

    @Override
    public void stop() throws Exception
    {
        try
        {
            if ( boundRaftId != null )
            {
                raftMessageDispatcher.deregisterHandlerChain( boundRaftId );
                boundRaftId = null;
            }
        }
        finally
        {
            delegateHandler.stop();
        }
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> message )
    {
        var raftId = boundRaftId;
        if ( Objects.isNull( raftId ) )
        {
            log.debug( "Message handling has been stopped, dropping the message: %s", message.message() );
        }
        else if ( !Objects.equals( raftId, message.raftId() ) )
        {
            log.info( "Discarding message[%s] owing to mismatched raftId. Expected: %s, Encountered: %s",
                    message.message(), raftId, message.raftId() );
        }
        else
        {
            delegateHandler.handle( message );
        }
    }
}
