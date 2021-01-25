/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class BenchmarkPlan
{
    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;
    private final Plan plan;

    @JsonCreator
    public BenchmarkPlan( @JsonProperty( "benchmarkGroup" ) BenchmarkGroup benchmarkGroup,
                          @JsonProperty( "benchmark" ) Benchmark benchmark,
                          @JsonProperty( "plan" ) Plan plan )
    {
        this.benchmarkGroup = benchmarkGroup;
        this.benchmark = benchmark;
        this.plan = plan;
    }

    public BenchmarkGroup benchmarkGroup()
    {
        return benchmarkGroup;
    }

    public Benchmark benchmark()
    {
        return benchmark;
    }

    public Plan plan()
    {
        return plan;
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
        BenchmarkPlan that = (BenchmarkPlan) o;
        return Objects.equals( benchmarkGroup, that.benchmarkGroup ) &&
               Objects.equals( benchmark, that.benchmark ) &&
               Objects.equals( plan, that.plan );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( benchmarkGroup, benchmark, plan );
    }

    @Override
    public String toString()
    {
        return "(\n" +
               "\tbenchmarkGroup=" + benchmarkGroup.name() +
               "\tbenchmark=" + benchmark.name() +
               "\tplan=\n" + plan.planTree().asciiPlanDescription() +
               ")";
    }
}
