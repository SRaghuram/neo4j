/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.io.UncheckedIOException;

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
    public boolean visit( SegmentFile segment )
    {
        if ( accumulatedSize < bytesToKeep )
        {
            file = segment;
            try
            {
                accumulatedSize += file.size();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            return false;
        }

        return true;
    }
}
