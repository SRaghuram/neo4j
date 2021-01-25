/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.neo4j.internal.helpers.collection.Visitor;

class SizeBasedLogPruningStrategy implements CoreLogPruningStrategy, Visitor<SegmentFile,RuntimeException>
{
    private final long bytesToKeep;
    private long accumulatedSize;
    private SegmentFile file;

    SizeBasedLogPruningStrategy( long bytesToKeep )
    {
        this.bytesToKeep = bytesToKeep;
    }

    @Override
    public synchronized long getIndexToKeep( Segments segments )
    {
        accumulatedSize = 0;
        file = null;

        segments.visitBackwards( this );

        return file != null ? (file.header().prevIndex() + 1) : -1;
    }

    @Override
    public boolean visit( SegmentFile segment ) throws RuntimeException
    {
        if ( accumulatedSize < bytesToKeep )
        {
            file = segment;
            accumulatedSize += file.size();
            return false;
        }

        return true;
    }
}
