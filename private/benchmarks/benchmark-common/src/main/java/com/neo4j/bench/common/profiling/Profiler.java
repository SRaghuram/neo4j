/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

public interface Profiler
{
    default String description()
    {
        return "Profiler: " + getClass().getName();
    }
}
