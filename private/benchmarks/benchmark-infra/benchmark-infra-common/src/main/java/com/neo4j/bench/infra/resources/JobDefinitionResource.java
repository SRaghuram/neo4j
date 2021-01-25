/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

public class JobDefinitionResource extends InfrastructureResource
{
    private final String jobDefinitionName;

    JobDefinitionResource( String jobDefinitionName,
                           int availableCores,
                           long totalMemory,
                           String jdkVendor,
                           String jdkEdition )

    {
        super( Hardware.availableCores( availableCores ), Hardware.totalMemory( totalMemory ), Jdk.of( jdkVendor, jdkEdition ) );
        this.jobDefinitionName = jobDefinitionName;
    }

    public String jobDefinitionName()
    {
        return jobDefinitionName;
    }
}
