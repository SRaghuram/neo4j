/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;

public class InfrastructureMatcher
{

    private final InfrastructureResources infrastructureResources;

    public InfrastructureMatcher( InfrastructureResources infrastructureResources )
    {

        this.infrastructureResources = infrastructureResources;
    }

    /**
     * Finds matching {@link Infrastructure} which has expected set of infrastructure capabilities.
     *
     * @param stackName                           CloudFormation stack name
     * @param requestedInfrastructureCapabilities set of expected infrastructure capabilities
     * @return matched infrastructure
     * @throws RuntimeException when Infrastructure not found
     */
    public Infrastructure findInfrastructure( String stackName, InfrastructureCapabilities requestedInfrastructureCapabilities )
    {

        Jdk jdk = requestedInfrastructureCapabilities.capabilityOf( Jdk.class )
                                                     .orElseThrow( () -> new IllegalArgumentException( "jdk capability is required" ) );

        if ( requestedInfrastructureCapabilities.hasAllCapabilties( AWS.InstanceType.class,
                                                                    Hardware.AvailableCores.class,
                                                                    Hardware.TotalMemory.class ) )
        {
            throw new IllegalArgumentException( "either instanceType or (availableCores and totalMemory) are required, you cannot supply both" );
        }

        // we match either by instanceType or (totalMemory and availableCores)
        Set<InfrastructureCapability<?>> requestedCapabilities = requestedInfrastructureCapabilities.capabilityOf( AWS.InstanceType.class )
                                                                                                    .map( Collections::<InfrastructureCapability<?>>singleton )
                                                                                                    .orElseGet(
                                                                                                            totalMemoryAndAvailableCores(
                                                                                                                    requestedInfrastructureCapabilities ) );

        Collection<InfrastructureResource> resources = infrastructureResources.findResources( stackName );
        Set<ComputeEnvironmentResource> computeEnvironmentResources =
                queryForResourcesWithCapabilities( resources,
                                                   ComputeEnvironmentResource.class,
                                                   requestedCapabilities );
        if ( computeEnvironmentResources.size() > 1 )
        {
            throw new RuntimeException( format( "More than one compute environment found, matching the capabilities: %s", requestedCapabilities ) );
        }

        ComputeEnvironmentResource computeEnvironmentResource =
                computeEnvironmentResources.stream()
                                           .findFirst()
                                           .orElseThrow( () -> new RuntimeException( format( "no matching capabilities %s compute environment found",
                                                                                             requestedCapabilities ) ) );

        Hardware.TotalMemory totalMemory = computeEnvironmentResource.getCapabilityFor( Hardware.TotalMemory.class );
        Hardware.AvailableCores availableCores = computeEnvironmentResource.getCapabilityFor( Hardware.AvailableCores.class );

        Set<JobDefinitionResource> jobDefinitionResources = queryForResourcesWithCapabilities( resources,
                                                                                               JobDefinitionResource.class,
                                                                                               ImmutableSet.of( availableCores,
                                                                                                                jdk,
                                                                                                                totalMemory ) );

        if ( jobDefinitionResources.isEmpty() )
        {
            throw new InfrastructureMatchingException(
                    format( "no matching job definition ( %s ) found", asList( availableCores, totalMemory, jdk ) ) );
        }

        if ( jobDefinitionResources.size() > 1 )
        {
            throw new InfrastructureMatchingException( format( "ambiguous job definition ( %s ) found in ( %s )",
                                                asList( availableCores, totalMemory, jdk ),
                                                jobDefinitionResources ) );
        }

        return new Infrastructure( computeEnvironmentResource.queueName(), jobDefinitionResources.stream().findFirst().get().jobDefinitionName() );
    }

    private static <T extends InfrastructureResource> Set<T> queryForResourcesWithCapabilities(
            Collection<InfrastructureResource> infrastructureResources,
            Class<T> resourceType,
            Set<InfrastructureCapability<?>> capabilities )
    {

        return infrastructureResources.stream()
                                      .filter( resourceType::isInstance )
                                      .map( resourceType::cast )
                                      .filter( resource -> capabilities.stream()
                                                                       .map( InfrastructureCapability::predicate )
                                                                       .allMatch(
                                                                               resourceHasCapabilities( resource ) ) )
                                      .collect( toSet() );
    }

    private static <T extends InfrastructureResource> Predicate<Predicate<InfrastructureCapability<?>>> resourceHasCapabilities( T resource )
    {
        return capabilityPredicate -> resource.capabilities()
                                              .stream()
                                              .anyMatch( capabilityPredicate::test );
    }

    private static Supplier<Set<InfrastructureCapability<?>>> totalMemoryAndAvailableCores( InfrastructureCapabilities infrastructureCapabilities )
    {
        return () ->
        {
            Hardware.TotalMemory totalMemory =
                    infrastructureCapabilities.capabilityOf( Hardware.TotalMemory.class )
                                              .orElseThrow( () -> new IllegalArgumentException( "total memory capability not found" ) );
            Hardware.AvailableCores availableCores =
                    infrastructureCapabilities.capabilityOf( Hardware.AvailableCores.class )
                                              .orElseThrow( () -> new IllegalArgumentException( "available cores capability not found" ) );
            return ImmutableSet.of( totalMemory, availableCores );
        };
    }
}
