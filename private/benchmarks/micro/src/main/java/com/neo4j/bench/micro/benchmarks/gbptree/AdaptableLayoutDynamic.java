/**
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
    public boolean fixedSize()
    {
        return false;
    }
}

