/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.tempindex;

public class CommentsTempIndex implements TempIndex
{
    private final TempIndex tempIndex;

    public CommentsTempIndex( TempIndex tempIndex )
    {
        this.tempIndex = tempIndex;
    }

    @Override
    public void put( long k, long v )
    {
        tempIndex.put( k, v );
    }

    @Override
    public long get( long k )
    {
        return tempIndex.get( k );
    }

    @Override
    public void shutdown()
    {
        tempIndex.shutdown();
    }
}
