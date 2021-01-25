/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

public class NativeMemoryTrackingCategory
{
    private final String category;
    private final long reserved;
    private final long committed;

    public NativeMemoryTrackingCategory( String category, long reserved, long committed )
    {
        this.category = category;
        this.reserved = reserved;
        this.committed = committed;
    }

    public String getCategory()
    {
        return category;
    }

    public long getReserved()
    {
        return reserved;
    }

    public long getCommitted()
    {
        return committed;
    }
}
