/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.util.HashMap;
import java.util.Map;

class AnnotationsFixture
{
    private final String packageName = "com.neo4j.bench.jmh.api.benchmarks";
    private Map<String,Annotations> annotationsCache = new HashMap<>();

    Annotations getInvalidAnnotations()
    {
        return getAnnotations( packageName + ".invalid" );
    }

    Annotations getValidAnnotations()
    {
        return getAnnotations( packageName + ".valid" );
    }

    Annotations getAnnotations()
    {
        return getAnnotations( packageName );
    }

    private Annotations getAnnotations( String aPackageName )
    {
        return annotationsCache.computeIfAbsent( aPackageName, Annotations::new );
    }
}
