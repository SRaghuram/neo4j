/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.nmt;

import java.util.Map;
import java.util.Set;

public class NativeMemoryTrackingSummary
{

    private final Map<String,NativeMemoryTrackingCategory> map;

    public NativeMemoryTrackingSummary( Map<String,NativeMemoryTrackingCategory> map )
    {
        this.map = map;
    }

    public NativeMemoryTrackingCategory getCategory( String string )
    {
        return map.get( string );
    }

    public Set<String> getCategories()
    {
        return map.keySet();
    }
}
