/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.timeout;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Timeout strategy that allows you to specify per message type what timeout to use
 */
public class MessageTimeoutStrategy
        implements TimeoutStrategy
{
    private Map<MessageType, Long> timeouts = new HashMap<>();

    private TimeoutStrategy delegate;

    public MessageTimeoutStrategy( TimeoutStrategy delegate )
    {
        this.delegate = delegate;
    }

    public MessageTimeoutStrategy timeout( MessageType messageType, long timeout )
    {
        timeouts.put( messageType, timeout );
        return this;
    }

    public MessageTimeoutStrategy relativeTimeout( MessageType messageType, MessageType relativeTo, long timeout )
    {
        timeouts.put( messageType, timeouts.get( relativeTo ) + timeout );
        return this;
    }

    @Override
    public long timeoutFor( Message message )
    {
        Long timeout = timeouts.get( message.getMessageType() );
        if ( timeout == null )
        {
            return delegate.timeoutFor( message );
        }
        else
        {
            return timeout;
        }
    }

    @Override
    public void timeoutTriggered( Message timeoutMessage )
    {
        delegate.timeoutTriggered( timeoutMessage );
    }

    @Override
    public void timeoutCancelled( Message timeoutMessage )
    {
        delegate.timeoutCancelled( timeoutMessage );
    }

    @Override
    public void tick( long now )
    {
        delegate.tick( now );
    }
}
