/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BenchmarkGroupBenchmarkPlans
{
    public static class BenchmarkPlans
    {
        @JsonSerialize( keyUsing = Benchmark.BenchmarkKeySerializer.class )
        @JsonDeserialize( keyUsing = Benchmark.BenchmarkKeyDeserializer.class )
        private final Map<Benchmark,Plan> inner = new HashMap<>();

        public boolean containsBenchmark( Benchmark benchmark )
        {
            return inner.containsKey( benchmark );
        }

        public Plan getPlanFor( Benchmark benchmark )
        {
            return inner.get( benchmark );
        }

        public void put( Benchmark benchmark, Plan plan )
        {
            if ( containsBenchmark( benchmark ) )
            {
                throw new IllegalStateException( format( "Multiple plans for benchmark: %s\nExisting: %s\nNew: %s",
                                                         benchmark, getPlanFor( benchmark ), plan ) );
            }
            inner.put( benchmark, plan );
        }

        public Collection<Benchmark> benchmarks()
        {
            return inner.keySet();
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    @JsonSerialize( keyUsing = BenchmarkGroup.BenchmarkGroupKeySerializer.class )
    @JsonDeserialize( keyUsing = BenchmarkGroup.BenchmarkGroupKeyDeserializer.class )
    private final Map<BenchmarkGroup,BenchmarkPlans> inner = new HashMap<>();

    public void add( BenchmarkGroup group, Benchmark benchmark, Plan plan )
    {
        requireNonNull( group );
        requireNonNull( benchmark );
        requireNonNull( plan );

        BenchmarkPlans groupBenchmarkPlans = inner.computeIfAbsent( group, key -> new BenchmarkPlans() );
        groupBenchmarkPlans.put( benchmark, plan );
    }

    public List<BenchmarkPlan> benchmarkPlans()
    {
        List<BenchmarkPlan> benchmarkPlans = new ArrayList<>();
        for ( Entry<BenchmarkGroup,BenchmarkPlans> groupEntry : inner.entrySet() )
        {
            for ( Benchmark benchmark : groupEntry.getValue().benchmarks() )
            {
                benchmarkPlans.add(
                        new BenchmarkPlan( groupEntry.getKey(), benchmark, groupEntry.getValue().getPlanFor( benchmark ) ) );
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
