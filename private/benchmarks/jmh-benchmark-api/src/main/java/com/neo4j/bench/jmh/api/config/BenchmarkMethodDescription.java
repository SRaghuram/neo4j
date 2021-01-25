/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.google.common.collect.Sets;
import org.openjdk.jmh.annotations.Mode;

import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * Describes one method of one benchmark (class that extends AbstractBenchmark),
 * including name and execution modes (e.g., sample time, throughput, average time, single shot).
 */
public class BenchmarkMethodDescription
{
    private final String name;
    private final Set<Mode> modes;

    BenchmarkMethodDescription( String name, Mode[] modes )
    {
        this.name = name;
        this.modes = Sets.newHashSet( modes );
    }

    public Set<BenchmarkMethodDescription> explode()
    {
        return modes.stream()
                .map( mode -> new BenchmarkMethodDescription(
                        name,
                        new Mode[]{mode} ) )
                .collect( toSet() );
    }

    public String name()
    {
        return name;
    }

    public Set<Mode> modes()
    {
        return modes;
    }

    @Override
    public String toString()
    {
        return format( "BenchmarkMethodDescription[%s, modes=%s]", name, modes );
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
        BenchmarkMethodDescription that = (BenchmarkMethodDescription) o;
        return Objects.equals( name, that.name ) &&
               Objects.equals( modes, that.modes );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, modes );
    }
}
