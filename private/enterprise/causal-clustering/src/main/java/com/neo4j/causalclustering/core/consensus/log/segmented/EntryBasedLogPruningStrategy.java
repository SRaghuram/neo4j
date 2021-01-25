/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class EntryBasedLogPruningStrategy implements CoreLogPruningStrategy
{
    private final long entriesToKeep;
    private final Log log;

    EntryBasedLogPruningStrategy( long entriesToKeep, LogProvider logProvider )
    {
        this.entriesToKeep = entriesToKeep;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public long getIndexToKeep( Segments segments )
    {
        SegmentVisitor visitor = new SegmentVisitor();
        segments.visitBackwards( visitor );

        if ( visitor.visitedCount == 0 )
        {
            log.warn( "No log files found during the prune operation. This state should resolve on its own, but" +
                      " if this warning continues, you may want to look for other errors in the user log." );
        }

        return visitor.prevIndex;
    }

    private class SegmentVisitor implements Visitor<SegmentFile,RuntimeException>
    {
        long visitedCount;
        long accumulated;
        long prevIndex = -1;
        long lastPrevIndex = -1;

        @Override
        public boolean visit( SegmentFile segment ) throws RuntimeException
        {
            visitedCount++;

            if ( lastPrevIndex == -1 )
            {
                lastPrevIndex = segment.header().prevIndex();
                return false; // first entry, continue visiting next
            }

            prevIndex = segment.header().prevIndex();
            accumulated += lastPrevIndex - prevIndex;
            lastPrevIndex = prevIndex;

            return accumulated >= entriesToKeep;
        }
    }
}
