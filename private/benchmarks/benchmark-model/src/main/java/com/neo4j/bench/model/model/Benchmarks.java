/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Benchmarks
{
    private final Benchmark parentBenchmark;
    private final List<Benchmark> childBenchmarks;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Benchmarks()
    {
        this( new Benchmark() );
    }

    public Benchmarks( Benchmark parentBenchmark )
    {
        this.parentBenchmark = parentBenchmark;
        this.childBenchmarks = new ArrayList<>();
    }

    public void addChildBenchmark( Benchmark childBenchmark )
    {
        childBenchmarks.add( childBenchmark );
    }

    public Benchmark parentBenchmark()
    {
        return parentBenchmark;
    }

    public boolean hasChildBenchmarkWith( String label )
    {
        return childBenchmarks.stream().anyMatch( benchmark -> benchmark.simpleName().endsWith( label ) );
    }

    public Benchmark childBenchmarkWith( String label )
    {
        List<Benchmark> matches = childBenchmarks.stream()
                                                 .filter( benchmark -> benchmark.simpleName().endsWith( ":" + label ) )
                                                 .collect( toList() );
        if ( matches.isEmpty() )
        {
            throw new RuntimeException( format( "Unexpected error. Found no matches for label: %s\n" +
                                                "Parent %s only contains children: %s",
                                                label, parentBenchmark.simpleName(),
                                                childBenchmarks.stream().map( Benchmark::simpleName ).collect( toList() ) ) );
        }
        if ( matches.size() > 1 )
        {
            throw new RuntimeException( format( "Unexpected error. Found multiple matches: %s",
                                                matches.stream().map( benchmark -> "\n" + benchmark.simpleName() ).collect( joining( ", " ) ) ) );
        }
        return matches.get( 0 );
    }

    public boolean isGroup()
    {
        return !childBenchmarks.isEmpty();
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
        Benchmarks that = (Benchmarks) o;
        return Objects.equals( parentBenchmark, that.parentBenchmark ) &&
               Objects.equals( childBenchmarks, that.childBenchmarks );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( parentBenchmark, childBenchmarks );
    }

    @Override
    public String toString()
    {
        return format( "Parent: %s%s",
                       parentBenchmark.simpleName(),
                       (isGroup())
                       ? "\n\t" + childBenchmarks.stream().map( Benchmark::simpleName ).collect( joining( "\n\t" ) )
                       : "" );
    }
}
