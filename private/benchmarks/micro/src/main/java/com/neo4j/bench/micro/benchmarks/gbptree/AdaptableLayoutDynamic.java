/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

class AdaptableLayoutDynamic extends AdaptableLayout
{
    AdaptableLayoutDynamic( int keySize, int valueSize )
    {
        super( keySize, valueSize );
    }

    @Override
    Layout layout()
    {
        return Layout.DYNAMIC;
    }

    @Override
    public int keySize( AdaptableKey adaptableKey )
    {
        return adaptableKey.totalSize;
    }

    @Override
    public boolean fixedSize()
    {
        return false;
    }

    @Override
    public void minimalSplitter( AdaptableKey left, AdaptableKey right, AdaptableKey into )
    {
        copyKey( right, into, AdaptableKey.DATA_SIZE );
    }
}

