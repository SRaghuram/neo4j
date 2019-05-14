/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.jmh.api.config.Annotations;

import java.util.HashMap;
import java.util.Map;

class AnnotationsFixture
{
    private final String packageName = "com.neo4j.bench.micro";
    private Map<String,Annotations> annotationsCache = new HashMap<>();

    Annotations getTestOnlyAnnotations()
    {
        return getAnnotations( packageName + ".benchmarks.test" );
    }

    Annotations getAnnotations()
    {
        return getAnnotations( packageName + ".benchmarks" );
    }

    private Annotations getAnnotations( String aPackageName )
    {
        return annotationsCache.computeIfAbsent( aPackageName, Annotations::new );
    }
}
