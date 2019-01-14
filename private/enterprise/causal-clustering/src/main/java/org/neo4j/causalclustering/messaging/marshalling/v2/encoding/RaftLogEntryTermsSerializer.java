/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2.encoding;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

class RaftLogEntryTermsSerializer
{
    static ByteBuf serializeTerms( RaftLogEntry[] raftLogEntries, ByteBufAllocator byteBufAllocator )
    {
        int capacity = Byte.SIZE + Integer.SIZE + Long.SIZE * raftLogEntries.length;
        ByteBuf buffer = byteBufAllocator.buffer( capacity, capacity );
        buffer.writeByte( ContentType.RaftLogEntryTerms.get() );
        buffer.writeInt( raftLogEntries.length );
        for ( RaftLogEntry raftLogEntry : raftLogEntries )
        {
            buffer.writeLong( raftLogEntry.term() );
        }
        return buffer;
    }
}
