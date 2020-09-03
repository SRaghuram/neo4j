/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Objects;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterBindingHandler implements LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
{
    private final LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> delegateHandler;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final Log log;

    private volatile RaftGroupId boundRaftGroupId;

    public ClusterBindingHandler( RaftMessageDispatcher raftMessageDispatcher,
            LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> delegateHandler,
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
    public void start( RaftGroupId raftGroupId ) throws Exception
    {
        boundRaftGroupId = raftGroupId;
        delegateHandler.start( raftGroupId );
        raftMessageDispatcher.registerHandlerChain( raftGroupId, this );
    }

    @Override
    public void stop() throws Exception
    {
        try
        {
            if ( boundRaftGroupId != null )
            {
                raftMessageDispatcher.deregisterHandlerChain( boundRaftGroupId );
                boundRaftGroupId = null;
            }
        }
        finally
        {
            delegateHandler.stop();
        }
    }

    @Override
    public void handle( RaftMessages.InboundRaftMessageContainer<?> message )
    {
        var raftGroupId = boundRaftGroupId;
        if ( Objects.isNull( raftGroupId ) )
        {
            log.debug( "Message handling has been stopped, dropping the message: %s", message.message() );
        }
        else if ( !Objects.equals( raftGroupId, message.raftGroupId() ) )
        {
            log.info( "Discarding message[%s] owing to mismatched raftGroupId. Expected: %s, Encountered: %s",
                    message.message(), raftGroupId, message.raftGroupId() );
        }
        else
        {
            delegateHandler.handle( message );
        }
    }
}
