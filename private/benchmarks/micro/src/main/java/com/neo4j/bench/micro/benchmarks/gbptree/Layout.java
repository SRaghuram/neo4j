/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

public enum Layout
{
    FIXED
            {
                @Override
                AdaptableLayout create( int keySize, int valueSize )
                {
                    return new AdaptableLayoutFixed( keySize, valueSize );
                }
            },
    DYNAMIC
            {
                @Override
                AdaptableLayout create( int keySize, int valueSize )
                {
                    return new AdaptableLayoutDynamic( keySize, valueSize );
                }
            };

    abstract AdaptableLayout create( int keySize, int valueSize );
}
