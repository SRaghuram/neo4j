/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;

import java.io.IOException;

import static java.lang.String.format;

public class InFlightLogEntryReader implements AutoCloseable
{
    private final ReadableRaftLog raftLog;
    private final InFlightCache inFlightCache;
    private final boolean pruneAfterRead;

    private RaftLogCursor cursor;
    private boolean useCache = true;

    public InFlightLogEntryReader( ReadableRaftLog raftLog, InFlightCache inFlightCache,
            boolean pruneAfterRead )
    {
        this.raftLog = raftLog;
        this.inFlightCache = inFlightCache;
        this.pruneAfterRead = pruneAfterRead;
    }

    public RaftLogEntry get( long logIndex ) throws IOException
    {
        RaftLogEntry entry = null;

        if ( useCache )
        {
            entry = inFlightCache.get( logIndex );
        }
        else
        {
            // report a cache miss
            inFlightCache.reportSkippedCacheAccess();
        }

        if ( entry == null )
        {
            /*
             * N.B.
             * This fallback code is strictly necessary since getUsingCursor() requires
             * that entries are accessed in strictly increasing order using a single cursor.
             */
            useCache = false;
            entry = getUsingCursor( logIndex );
        }

        if ( pruneAfterRead )
        {
            inFlightCache.prune( logIndex );
        }

        return entry;
    }

    private RaftLogEntry getUsingCursor( long logIndex ) throws IOException
    {
        if ( cursor == null )
        {
            cursor = raftLog.getEntryCursor( logIndex );
        }

        if ( cursor.next() )
        {
            if ( cursor.index() != logIndex )
            {
                throw new IllegalStateException( format( "expected index %d but was %s", logIndex, cursor.index() ) );
            }
            return cursor.get();
        }
        else
        {
            return null;
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }
}
