/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.jmh.api.config.BenchmarksFinder;

import java.util.HashMap;
import java.util.Map;

class AnnotationsFixture
{
    private final String packageName = "com.neo4j.bench.micro";
    private Map<String,BenchmarksFinder> annotationsCache = new HashMap<>();

    BenchmarksFinder getTestOnlyAnnotations()
    {
        return getAnnotations( packageName + ".benchmarks.test" );
    }

    BenchmarksFinder getAnnotations()
    {
        return getAnnotations( packageName + ".benchmarks" );
    }

    private BenchmarksFinder getAnnotations( String aPackageName )
    {
        return annotationsCache.computeIfAbsent( aPackageName, BenchmarksFinder::new );
    }
}
