/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BenchmarkGroupBenchmarkPlans
{
    private final Map<BenchmarkGroup,Map<Benchmark,Plan>> inner = new HashMap<>();

    public void add( BenchmarkGroup group, Benchmark benchmark, Plan plan )
    {
        requireNonNull( group );
        requireNonNull( benchmark );
        requireNonNull( plan );

        Map<Benchmark,Plan> groupBenchmarkPlans = inner.computeIfAbsent( group, key -> new HashMap<>() );
        if ( groupBenchmarkPlans.containsKey( benchmark ) )
        {
            throw new IllegalStateException( format( "Multiple plans for benchmark: %s\nExisting: %s\nNew: %s",
                    benchmark, groupBenchmarkPlans.get( benchmark ), plan ) );
        }
        groupBenchmarkPlans.put( benchmark, plan );
    }

    public List<BenchmarkPlan> benchmarkPlans()
    {
        List<BenchmarkPlan> benchmarkPlans = new ArrayList<>();
        for ( Entry<BenchmarkGroup,Map<Benchmark,Plan>> groupEntry : inner.entrySet() )
        {
            for ( Entry<Benchmark,Plan> benchmarkEntry : groupEntry.getValue().entrySet() )
            {
                benchmarkPlans.add(
                        new BenchmarkPlan( groupEntry.getKey(), benchmarkEntry.getKey(), benchmarkEntry.getValue() ) );
            }
        }
        return benchmarkPlans;
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
        BenchmarkGroupBenchmarkPlans that = (BenchmarkGroupBenchmarkPlans) o;
        return Objects.equals( inner, that.inner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( inner );
    }

    @Override
    public String toString()
    {
        return "BenchmarkGroupBenchmarkPlans{" + "inner=" + inner + '}';
    }
}
