package com.neo4j.bench.jmh.api.config;

import java.util.HashMap;
import java.util.Map;

public class AnnotationsFixture
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
