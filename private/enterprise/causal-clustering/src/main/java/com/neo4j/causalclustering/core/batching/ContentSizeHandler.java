/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

class ContentSizeHandler extends RaftMessages.HandlerAdaptor<Long,RuntimeException>
{
    private static final ContentSizeHandler INSTANCE = new ContentSizeHandler();

    private ContentSizeHandler()
    {
    }

    static long of( RaftMessages.InboundRaftMessageContainer<?> messageContainer )
    {
        var dispatch = messageContainer.message().dispatch( INSTANCE );
        return dispatch == null ? 0L : dispatch;
    }

    @Override
    public Long handle( RaftMessages.NewEntry.Request request ) throws RuntimeException
    {
        return request.content().size().orElse( 0L );
    }

    @Override
    public Long handle( RaftMessages.AppendEntries.Request request ) throws RuntimeException
    {
        long totalSize = 0L;
        for ( RaftLogEntry entry : request.entries() )
        {
            if ( entry.content().size().isPresent() )
            {
                totalSize += entry.content().size().getAsLong();
            }
        }
        return totalSize;
    }
}
