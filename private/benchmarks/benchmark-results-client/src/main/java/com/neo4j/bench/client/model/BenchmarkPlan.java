package com.neo4j.bench.client.model;

import java.util.Objects;

public class BenchmarkPlan
{
    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;
    private final Plan plan;

    public BenchmarkPlan()
    {
        this( new BenchmarkGroup(), new Benchmark(), new Plan() );
    }

    public BenchmarkPlan( BenchmarkGroup benchmarkGroup, Benchmark benchmark, Plan plan )
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
