/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Benchmarking infrastructure resource's names
 */
public class Infrastructure
{
    private final String jobQueue;
    private final String jobDefinition;

    public Infrastructure( String jobQueue, String jobDefinition )
    {
        this.jobQueue = jobQueue;
        this.jobDefinition = jobDefinition;
    }

    public String jobQueue()
    {
        return jobQueue;
    }

    public String jobDefinition()
    {
        return jobDefinition;
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
