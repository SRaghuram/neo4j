package com.neo4j.bench.micro.benchmarks.values;

import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;

public abstract class AbstractValuesBenchmark extends BaseRegularBenchmark
{
    @Override
    public String benchmarkGroup()
    {
        return "Values";
    }
}
