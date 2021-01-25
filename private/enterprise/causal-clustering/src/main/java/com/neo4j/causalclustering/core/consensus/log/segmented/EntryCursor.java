/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.consensus.log.segmented.OpenEndRangeMap.ValueRange;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;

/**
 * The entry store allows iterating over RAFT log entries efficiently and handles moving from one
 * segment to the next in a transparent manner. It can thus be mainly viewed as a factory for a
 * smart segment-crossing cursor.
 */
class EntryCursor implements IOCursor<EntryRecord>
{
    private final Segments segments;
    private IOCursor<EntryRecord> cursor;
    private ValueRange<Long,SegmentFile> segmentRange;
    private long currentIndex;

    private long limit = Long.MAX_VALUE;
    private CursorValue<EntryRecord> currentRecord = new CursorValue<>();

    EntryCursor( Segments segments, long logIndex )
    {
        this.segments = segments;
        this.currentIndex = logIndex - 1;
    }

    @Override
    public boolean next() throws IOException
    {
        currentIndex++;
        if ( segmentRange == null || currentIndex >= limit )
        {
            if ( !nextSegment() )
            {
                return false;
            }
        }

        if ( cursor.next() )
        {
            currentRecord.set( cursor.get() );
            return true;
        }

        currentRecord.invalidate();
        return false;
    }

    private boolean nextSegment() throws IOException
    {
        segmentRange = segments.getForIndex( currentIndex );
        Optional<SegmentFile> optionalFile = segmentRange.value();

        if ( !optionalFile.isPresent() )
        {
            currentRecord.invalidate();
            return false;
        }

        SegmentFile file = optionalFile.get();

        /* Open new reader before closing old, so that pruner cannot overtake us. */
        IOCursor<EntryRecord> oldCursor = cursor;
        try
        {
            cursor = file.getCursor( currentIndex );
        }
        catch ( DisposedException e )
        {
            currentRecord.invalidate();
            return false;
        }

        if ( oldCursor != null )
        {
            oldCursor.close();
        }

        limit = segmentRange.limit().orElse( Long.MAX_VALUE );

        return true;
    }

    @Override
    public void close() throws IOException
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }

    @Override
    public EntryRecord get()
    {
        return currentRecord.get();
    }
}
