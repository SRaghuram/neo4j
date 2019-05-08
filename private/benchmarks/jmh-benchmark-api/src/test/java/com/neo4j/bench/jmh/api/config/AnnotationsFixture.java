package com.neo4j.bench.jmh.api.config;

public class AnnotationsFixture
{
    private final String packageName = "com.neo4j.bench.jmh.api.benchmarks.test_only";
    private Annotations annotations;

    String getPackageName()
    {
        return packageName;
    }

    Annotations getAnnotations()
    {
        if ( null == annotations )
        {
            annotations = new Annotations( packageName );
        }
        return annotations;
    }
}
