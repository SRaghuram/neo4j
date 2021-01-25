/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
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
