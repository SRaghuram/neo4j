package com.neo4j.bench.micro.benchmarks.marketing;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;

public abstract class AbstractMarketingBenchmark extends BaseDatabaseBenchmark
{
    @Override
    public String benchmarkGroup()
    {
        return "Marketing";
    }
}
