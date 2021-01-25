/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Benchmark.Mode;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Benchmarks
{
    private static final String LABEL_SEPARATOR = ":";

    private final Benchmark parentBenchmark;
    private final List<Benchmark> childBenchmarks;

    static Benchmarks create( String benchmarkDescription,
                              String primaryName,
                              Collection<String> secondaryLabels,
                              Mode mode,
                              Map<String,String> parametersMap )
    {
        Benchmark parentBenchmark = Benchmark.benchmarkFor( benchmarkDescription, primaryName, mode, parametersMap );
        List<Benchmark> childBenchmarks = secondaryLabels
                .stream()
                .map( label -> Benchmark.benchmarkFor( benchmarkDescription, childLabel( primaryName, label ), mode, parametersMap ) )
                .collect( toList() );
        return new Benchmarks( parentBenchmark, childBenchmarks );
    }

    private static String childLabel( String primaryName, String label )
    {
        return primaryName + LABEL_SEPARATOR + label;
    }

    private Benchmarks( Benchmark parentBenchmark, List<Benchmark> childBenchmarks )
    {
        this.parentBenchmark = parentBenchmark;
        this.childBenchmarks = childBenchmarks;
    }

    public Benchmark parentBenchmark()
    {
        return parentBenchmark;
    }

    public boolean hasChildBenchmarkWith( String label )
    {
        return childBenchmarks.stream().anyMatch( benchmark -> benchmark.simpleName().endsWith( label ) );
    }

    public Set<Benchmark> childBenchmarks()
    {
        return new HashSet<>( childBenchmarks );
    }

    public Benchmark childBenchmarkWith( String label )
    {
        List<Benchmark> matches = childBenchmarks.stream()
                                                 .filter( benchmark -> benchmark.simpleName().endsWith( LABEL_SEPARATOR + label ) )
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
