/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

class AdaptableLayoutFixed extends AdaptableLayout
{
    AdaptableLayoutFixed( int keySize, int valueSize )
    {
        super( true, keySize, valueSize );
    }

    @Override
    Layout layout()
    {
        return Layout.FIXED;
    }

    @Override
    public int keySize( AdaptableKey adaptableKey )
    {
        return keySize;
    }

    @Override
    public void minimalSplitter( AdaptableKey left, AdaptableKey right, AdaptableKey into )
    {
        copyKey( right, into );
    }
}
