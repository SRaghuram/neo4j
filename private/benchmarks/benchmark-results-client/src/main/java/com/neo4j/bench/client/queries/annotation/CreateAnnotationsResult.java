package com.neo4j.bench.client.queries.annotation;

public class CreateAnnotationsResult
{
    private final long createdTestRunAnnotations;
    private final long createdMetricsAnnotations;

    CreateAnnotationsResult( long createdTestRunAnnotations, long createdMetricsAnnotations )
    {
        this.createdTestRunAnnotations = createdTestRunAnnotations;
        this.createdMetricsAnnotations = createdMetricsAnnotations;
    }

    public long createdTestRunAnnotations()
    {
        return createdTestRunAnnotations;
    }

    public long createdMetricsAnnotations()
    {
        return createdMetricsAnnotations;
    }
}
