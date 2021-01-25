/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

import java.util.Comparator;

class MessagePriority extends RaftMessages.HandlerAdaptor<Integer,RuntimeException>
        implements Comparator<RaftMessages.InboundRaftMessageContainer<?>>
{

    private final Integer BASE_PRIORITY = 10; // lower number means higher priority

    @Override
    public Integer handle( RaftMessages.AppendEntries.Request request )
    {
        // 0 length means this is a heartbeat, so let it be handled with higher priority
        return request.entries().length == 0 ? BASE_PRIORITY : 20;
    }

    @Override
    public Integer handle( RaftMessages.NewEntry.Request request )
    {
        return 30;
    }

    @Override
    public int compare( RaftMessages.InboundRaftMessageContainer<?> messageContainerA,
            RaftMessages.InboundRaftMessageContainer<?> messageContainerB )
    {
        int priorityA = getPriority( messageContainerA );
        int priorityB = getPriority( messageContainerB );

        return Integer.compare( priorityA, priorityB );
    }

    private int getPriority( RaftMessages.InboundRaftMessageContainer<?> messageContainer )
    {
        var priority = messageContainer.message().dispatch( this );
        return priority == null ? BASE_PRIORITY : priority;
    }
}
