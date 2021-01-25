/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

import java.io.IOException;

import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;

class SegmentedRaftLogCursor implements RaftLogCursor
{
    private final IOCursor<EntryRecord> inner;
    private CursorValue<RaftLogEntry> current;
    private long index;

    SegmentedRaftLogCursor( long fromIndex, IOCursor<EntryRecord> inner )
    {
        this.inner = inner;
        this.current = new CursorValue<>();
        this.index = fromIndex - 1;
    }

    @Override
    public boolean next() throws IOException
    {
        boolean hasNext = inner.next();
        if ( hasNext )
        {
            current.set( inner.get().logEntry() );
            index++;
        }
        else
        {
            current.invalidate();
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }

    @Override
    public long index()
    {
        return index;
    }

    @Override
    public RaftLogEntry get()
    {
        return current.get();
    }
}
