/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfrastructureMatcherTest
{

    @Test
    public void matchInfrastructureByMemoryAndCores()
    {
        // given
        InfrastructureResources infrastructureResources = mock( InfrastructureResources.class );
        when( infrastructureResources.findResources( "stackName" ) )
                .thenReturn(
                        ImmutableList.of(
                                new ComputeEnvironmentResource( "queueName", 8, 16384, "r5d.large" ),
                                new ComputeEnvironmentResource( "queueName-larger", 16, 16384, "r5d.2xlarge" ),
                                new JobDefinitionResource( "jobDefinition", 8, 16000, "oracle", "8" ),
                                new JobDefinitionResource( "jobDefinition-smaller", 16, 8192, "oracle", "8" )
                        )
                );

        InfrastructureMatcher infrastructureMatcher = new InfrastructureMatcher( infrastructureResources );

        // when
        Infrastructure infraMatch = infrastructureMatcher.findInfrastructure( "stackName",
                                                                              InfrastructureCapabilities.empty()
                                                                                                        .withCapability( Hardware.totalMemory( 16384 ) )
                                                                                                        .withCapability( Hardware.availableCores( 8 ) )
                                                                                                        .withCapability( Jdk.of( "oracle", "8" ) ) );

        // then
        assertEquals( "queueName", infraMatch.jobQueue() );
        assertEquals( "jobDefinition", infraMatch.jobDefinition() );
    }

    @Test
    public void matchInfrastructureByInstanceType()
    {
        // given
        InfrastructureResources infrastructureResources = mock( InfrastructureResources.class );
        when( infrastructureResources.findResources( "stackName" ) )
                .thenReturn(
                        ImmutableList.of(
                                new ComputeEnvironmentResource( "queueName", 8, 16384, "r5d.large" ),
                                new ComputeEnvironmentResource( "queueName-larger", 16, 16384, "r5d.2xlarge" ),
                                new JobDefinitionResource( "jobDefinition", 16, 16000, "oracle", "8" ),
                                new JobDefinitionResource( "jobDefinition-smaller", 8, 8192, "oracle", "8" )
                        )
                );

        InfrastructureMatcher infrastructureMatcher = new InfrastructureMatcher( infrastructureResources );

        // when
        Infrastructure infraMatch = infrastructureMatcher.findInfrastructure( "stackName",
                                                                              InfrastructureCapabilities.empty()
                                                                                                        .withCapability( AWS.instanceType( "r5d.2xlarge" ) )
                                                                                                        .withCapability( Jdk.of( "oracle", "8" ) ) );

        // then
        assertEquals( "queueName-larger", infraMatch.jobQueue() );
        assertEquals( "jobDefinition", infraMatch.jobDefinition() );
    }

    @Test
    public void failOnMatchInfrastructureByInstanceTypeAndCPUAndMemory()
    {
        // given
        InfrastructureResources infrastructureResources = mock( InfrastructureResources.class );
        InfrastructureMatcher infrastructureMatcher = new InfrastructureMatcher( infrastructureResources );
        // when
        assertThrows(
                IllegalArgumentException.class,
                () -> infrastructureMatcher.findInfrastructure( "stackName", InfrastructureCapabilities.empty()
                                                                                                       .withCapability( AWS.instanceType( "m5d.large" ) )
                                                                                                       .withCapability( Hardware.availableCores( 8 ) )
                                                                                                       .withCapability( Hardware.totalMemory( 8192 ) )
                                                                                                       .withCapability( Jdk.of( "oracle-8" ) ) ),
                "either instanceType or (availableCores and totalMemory) are required, you cannot supply both"
        );
    }

    @Test
    public void failOnNoMatchingInfrastructure()
    {
        // given
        InfrastructureResources infrastructureResources = mock( InfrastructureResources.class );
        when( infrastructureResources.findResources( "stackName" ) )
                .thenReturn(
                        ImmutableList.of(
                                new ComputeEnvironmentResource( "queueName", 8, 16384, "r5d.large" ),
                                new ComputeEnvironmentResource( "queueName-larger", 16, 16384, "r5d.2xlarge" ),
                                new JobDefinitionResource( "jobDefinition", 8, 16000, "oracle", "8" ),
                                new JobDefinitionResource( "jobDefinition-smaller", 8, 8192, "oracle", "8" )
                        )
                );

        InfrastructureMatcher infrastructureMatcher = new InfrastructureMatcher( infrastructureResources );

        // when
        assertThrows( InfrastructureMatchingException.class,
                      () -> infrastructureMatcher.findInfrastructure( "stackName",
                                                                      InfrastructureCapabilities.empty()
                                                                                                .withCapability( Hardware.totalMemory( 16384 ) )
                                                                                                .withCapability( Hardware.availableCores( 8 ) )
                                                                                                .withCapability( Jdk.of( "oracle", "11" ) ) ),
                      "no matching job definition ( [Hardware.AvailableCores[8], Hardware.TotalMemory[16384], Jdk[oracle-11]] ) found" );
    }

    @Test
    public void failOnMatchingMoreThenOneInfrastructure()
    {
        // given
        InfrastructureResources infrastructureResources = mock( InfrastructureResources.class );
        when( infrastructureResources.findResources( "stackName" ) )
                .thenReturn(
                        ImmutableList.of(
                                new ComputeEnvironmentResource( "queueName", 8, 8192, "r5d.large" ),
                                new ComputeEnvironmentResource( "queueName-larger", 8, 16384, "r5d.2xlarge" ),
                                new JobDefinitionResource( "jobDefinition", 8, 8000, "oracle", "8" ),
                                new JobDefinitionResource( "jobDefinition-smaller", 8, 8192, "oracle", "8" )
                        )
                );

        InfrastructureMatcher infrastructureMatcher = new InfrastructureMatcher( infrastructureResources );

        // when
        assertThrows( InfrastructureMatchingException.class,
                      () -> infrastructureMatcher.findInfrastructure( "stackName",
                                                                      InfrastructureCapabilities.empty()
                                                                                                .withCapability( Hardware.totalMemory( 8192 ) )
                                                                                                .withCapability( Hardware.availableCores( 8 ) )
                                                                                                .withCapability( Jdk.of( "oracle", "8" ) ) ),
                      "ambiguous job definition ( [Hardware.AvailableCores[8], Hardware.TotalMemory[8192], Jdk[oracle-8]] ) found in " +
                      "( [JobDefinitionResource[jobDefinitionName=jobDefinition,capabilities=[Hardware.AvailableCores[8], Hardware.TotalMemory[8000], " +
                      "Jdk[oracle-8]]], JobDefinitionResource[jobDefinitionName=jobDefinition-smaller,capabilities=[Hardware.AvailableCores[8], " +
                      "Hardware.TotalMemory[8192], Jdk[oracle-8]]]] )" );
    }
}
