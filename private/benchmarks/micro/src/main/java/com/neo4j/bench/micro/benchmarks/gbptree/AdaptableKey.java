/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

class AdaptableKey
{
    static final int DATA_SIZE = Long.BYTES;
    int totalSize;
    long value;

    AdaptableKey( int totalSize )
    {
        this.totalSize = totalSize;
    }

    void copyFrom( AdaptableKey other, int targetLength )
    {
        this.totalSize = targetLength;
        this.value = other.value;
    }

    int padding()
    {
        return totalSize - DATA_SIZE;
    }

    @Override
    public String toString()
    {
        return Long.toString(  value );
    }
}
