package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State( Scope.Benchmark )
public abstract class AbstractCoreBenchmark extends BaseDatabaseBenchmark
{
    @Override
    public String benchmarkGroup()
    {
        return "Core API";
    }
}
