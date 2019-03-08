/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Objects;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterBindingHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegateHandler;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final Log log;

    private volatile ClusterId boundClusterId;

    public ClusterBindingHandler( RaftMessageDispatcher raftMessageDispatcher,
            LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegateHandler,
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
    public void start( ClusterId clusterId ) throws Exception
    {
        boundClusterId = clusterId;
        delegateHandler.start( clusterId );
        raftMessageDispatcher.registerHandlerChain( boundClusterId, this );
    }

    @Override
    public void stop() throws Exception
    {
        try
        {
            delegateHandler.stop();
        }
        finally
        {
            raftMessageDispatcher.deregisterHandlerChain( boundClusterId );
            boundClusterId = null;
        }
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
    {
        if ( Objects.isNull( boundClusterId ) )
        {
            log.debug( "Message handling has been stopped, dropping the message: %s", message.message() );
        }
        else if ( !Objects.equals( boundClusterId, message.clusterId() ) )
        {
            log.info( "Discarding message[%s] owing to mismatched clusterId. Expected: %s, Encountered: %s",
                    message.message(), boundClusterId, message.clusterId() );
        }
        else
        {
            delegateHandler.handle( message );
        }
    }
}
