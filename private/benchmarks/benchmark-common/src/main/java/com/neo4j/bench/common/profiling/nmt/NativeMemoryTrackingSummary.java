/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

import java.util.Map;
import java.util.Set;

public class NativeMemoryTrackingSummary
{

    private final Map<String,NativeMemoryTrackingCategory> categories;

    public NativeMemoryTrackingSummary( Map<String,NativeMemoryTrackingCategory> categories )
    {
        this.categories = categories;
    }

    public NativeMemoryTrackingCategory getCategory( String category )
    {
        return categories.get( category );
    }

    public Set<String> getCategories()
    {
        return categories.keySet();
    }
}
