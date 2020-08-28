/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

public class ComputeEnvironmentResource extends InfrastructureResource
{
    private final String queueName;

    ComputeEnvironmentResource( String queueName, int availableCores, long totalMemory, String instanceType )
    {
        super( Hardware.availableCores( availableCores ), Hardware.totalMemory( totalMemory ), AWS.instanceType( instanceType ) );
        this.queueName = queueName;
    }

    public String queueName()
    {
        return queueName;
    }
}
