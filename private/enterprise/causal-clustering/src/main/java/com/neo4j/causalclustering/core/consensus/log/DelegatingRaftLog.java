/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

public class DelegatingRaftLog implements RaftLog
{
    private final RaftLog inner;

    public DelegatingRaftLog( RaftLog inner )
    {
        this.inner = inner;
    }

    @Override
    public long append( RaftLogEntry... entry ) throws IOException
    {
        return inner.append( entry );
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        inner.truncate( fromIndex );
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        return inner.prune( safeIndex );
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        return inner.skip( index, term );
    }

    @Override
    public long appendIndex()
    {
        return inner.appendIndex();
    }

    @Override
    public long prevIndex()
    {
        return inner.prevIndex();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        return inner.readEntryTerm( logIndex );
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        return inner.getEntryCursor( fromIndex );
    }
}
