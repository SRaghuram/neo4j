/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.time.Instant;
import java.util.OptionalLong;

import org.neo4j.kernel.database.DatabaseIdFactory;

class Helpers
{
    private static final java.util.UUID UUID = java.util.UUID.randomUUID();
    private static final RaftId RAFT_ID = RaftId.from( DatabaseIdFactory.from( UUID ) );
    private static final MemberId MEMBER_ID = new MemberId( UUID );
    private static final Instant INSTANT = Instant.MAX;

    static ReplicatedContent emptyContent()
    {
        return new ReplicatedContent()
        {
            @Override
            public OptionalLong size()
            {
                return OptionalLong.empty();
            }

            @Override
            public void dispatch( ReplicatedContentHandler contentHandler )
            {
                // do nothing
            }
        };
    }

    static ReplicatedContent contentWithOneByte()
    {
        return new ReplicatedContent()
        {
            @Override
            public OptionalLong size()
            {
                return OptionalLong.of( 1 );
            }

            @Override
            public void dispatch( ReplicatedContentHandler contentHandler )
            {
                // do nothing
            }
        };
    }

    static RaftMessages.InboundRaftMessageContainer<RaftMessages.NewEntry.Request> message( ReplicatedContent content )
    {
        return RaftMessages.InboundRaftMessageContainer.of( INSTANT, RAFT_ID, new RaftMessages.NewEntry.Request( MEMBER_ID, content ) );
    }

    static RaftMessages.InboundRaftMessageContainer<RaftMessages.AppendEntries.Request> message( long prevLogIndex,
            RaftLogEntry[] entries )
    {
        return RaftMessages.InboundRaftMessageContainer
                .of( INSTANT, RAFT_ID, new RaftMessages.AppendEntries.Request( MEMBER_ID, 1, prevLogIndex, 1, entries, 1 ) );
    }
}
