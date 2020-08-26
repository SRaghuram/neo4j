/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutboundMessageCollector implements Outbound<RaftMemberId, RaftMessages.RaftMessage>
{
    private Map<RaftMemberId, List<RaftMessages.RaftMessage>> sentMessages = new HashMap<>();

    public void clear()
    {
        sentMessages.clear();
    }

    @Override
    public void send( RaftMemberId to, RaftMessages.RaftMessage message, boolean block )
    {
        raftMessages( to ).add( message );
    }

    private List<RaftMessages.RaftMessage> raftMessages( RaftMemberId to )
    {
        return sentMessages.computeIfAbsent( to, k -> new ArrayList<>() );
    }

    public List<RaftMessages.RaftMessage> sentTo( RaftMemberId member )
    {
        List<RaftMessages.RaftMessage> messages = sentMessages.get( member );

        if ( messages == null )
        {
            messages = new ArrayList<>();
        }

        return messages;
    }

    public boolean hasAnyEntriesTo( RaftMemberId member )
    {
        List<RaftMessages.RaftMessage> messages = sentMessages.get( member );
        return messages != null && messages.size() != 0;
    }
}
