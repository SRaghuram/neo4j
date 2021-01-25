/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.util.HashMap;
import java.util.Map;

class BenchmarksFinderFixture
{
    private final String packageName = "com.neo4j.bench.jmh.api.benchmarks";
    private Map<String,BenchmarksFinder> cache = new HashMap<>();

    BenchmarksFinder getInvalidBenchmarksFinder()
    {
        return getBenchmarksFinder( packageName + ".invalid" );
    }

    BenchmarksFinder getValidBenchmarksFinder()
    {
        return getBenchmarksFinder( packageName + ".valid" );
    }

    BenchmarksFinder getBenchmarksFinder()
    {
        return getBenchmarksFinder( packageName );
    }

    private BenchmarksFinder getBenchmarksFinder( String aPackageName )
    {
        return cache.computeIfAbsent( aPackageName, BenchmarksFinder::new );
    }
}
