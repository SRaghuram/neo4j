/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * Describes one parameter of one benchmark (class that extends AbstractBenchmark),
 * including name, allowed values, and actual values.
 */
public class BenchmarkParamDescription
{
    private final String name;
    private final Set<String> allowedValues;
    private final Set<String> values;

    public BenchmarkParamDescription( String name, Set<String> allowedValues, Set<String> values )
    {
        this.name = name;
        this.allowedValues = allowedValues;
        this.values = values;
    }

    public BenchmarkParamDescription withValues( Set<String> newValues )
    {
        return new BenchmarkParamDescription( name, allowedValues, newValues );
    }

    public String name()
    {
        return name;
    }

    public Set<String> allowedValues()
    {
        return allowedValues;
    }

    public Set<String> values()
    {
        return values;
    }

    public String[] valuesArray()
    {
        return values.toArray( new String[0] );
    }

    public Set<BenchmarkParamDescription> explode()
    {
        return values.stream()
                .map( value -> new BenchmarkParamDescription(
                        name,
                        allowedValues,
                        newHashSet( value ) ) )
                .collect( toSet() );
    }

    @Override
    public String toString()
    {
        return format( "(%s, allowed=%s, base=%s)", name, allowedValues, values );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkParamDescription that = (BenchmarkParamDescription) o;
        return Objects.equals( name, that.name ) &&
               Objects.equals( allowedValues, that.allowedValues ) &&
               Objects.equals( values, that.values );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, allowedValues, values );
    }
}
