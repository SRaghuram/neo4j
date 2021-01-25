/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.pageCache;

import com.neo4j.bench.micro.benchmarks.RNGState;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

public abstract class WriteV2 extends AbstractPageCacheBenchmarkV2
{
    public void randomWrite( CursorState cursorState, RNGState rngState ) throws IOException
    {
        long id = cursorState.id( rngState.rng );
        PageCursor cursor = cursorState.pageCursor;
        if ( cursor.next( id ) )
        {
            // pinned, yay!
            cursor.putByte( (byte) 1 );
        }
        else
        {
            throw new IllegalStateException( "Did not expect next() to return false" );
        }
    }
}
