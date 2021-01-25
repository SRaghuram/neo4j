/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

/**
 * Set of infrastructure capabilities.
 */
public class InfrastructureCapabilities
{

    private static final Map<String,Function<String,? extends InfrastructureCapability>> CAPABILITY_NAMES = ImmutableMap.of(
            "hardware:totalMemory", Hardware::totalMemory,
            "hardware:availableCores", Hardware::availableCores,
            "jdk", Jdk::of,
            "AWS:instanceType", AWS::instanceType
    );

    public static InfrastructureCapabilities fromArgs( String args )
    {

        Map<String,String> parsedArgs = Arrays.stream( args.split( "," ) )
                                              .map( arg -> arg.split( "=" ) )
                                              .filter( arg -> arg.length == 2 )
                                              .collect( toMap( arg -> arg[0].trim(), arg -> arg[1].trim() ) );

        if ( parsedArgs.isEmpty() )
        {
            throw new IllegalArgumentException( format( "invalid capabilities args '%s'", args ) );
        }

        InfrastructureCapabilities infrastructureCapabilities = InfrastructureCapabilities.empty();
        for ( Map.Entry<String,String> entry : parsedArgs.entrySet() )
        {
            infrastructureCapabilities = infrastructureCapabilities.withCapability(
                    ofNullable( CAPABILITY_NAMES.get( entry.getKey() ) )
                            .map( f -> f.apply( entry.getValue() ) )
                            .orElseThrow(
                                    () -> new IllegalArgumentException( format( "Unknown \"%s\" capability", entry.getKey() ) ) ) );
        }
        return infrastructureCapabilities;
    }

    private final ImmutableMap<Class<? extends InfrastructureCapability>,InfrastructureCapability> capabilities;

    private InfrastructureCapabilities( ImmutableMap<Class<? extends InfrastructureCapability>,InfrastructureCapability> capabilities )
    {
        this.capabilities = capabilities;
    }

    public static InfrastructureCapabilities empty()
    {
        return new InfrastructureCapabilities( ImmutableMap.of() );
    }

    public InfrastructureCapabilities withCapability( InfrastructureCapability capability )
    {
        Class<? extends InfrastructureCapability> aClass = capability.getClass();
        if ( capabilities.containsKey( aClass ) )
        {
            throw new IllegalArgumentException( format( "capability of %s already exists", aClass ) );
        }
        return new InfrastructureCapabilities( ImmutableMap.<Class<? extends InfrastructureCapability>,InfrastructureCapability>builder()
                                                       .putAll( capabilities )
                                                       .put( aClass, capability )
                                                       .build() );
    }

    public <T extends InfrastructureCapability> Optional<T> capabilityOf( Class<T> aClass )
    {
        return ofNullable( capabilities.get( aClass ) ).map( aClass::cast );
    }

    public Set<? extends InfrastructureCapability> capabilities()
    {
        return ImmutableSet.copyOf( capabilities.values() );
    }

    public boolean hasAllCapabilties( Class<? extends InfrastructureCapability>... infra )
    {
        return Arrays.stream( infra ).allMatch( type -> capabilities.values().stream().anyMatch( capability -> type.isInstance( capability ) ) );
    }
}
