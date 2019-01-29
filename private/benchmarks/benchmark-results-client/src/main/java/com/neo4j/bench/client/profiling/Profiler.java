package com.neo4j.bench.client.profiling;

public interface Profiler
{
    default String description()
    {
        return "Profiler: " + getClass().getName();
    }
}
