package com.neo4j.bench.jmh.api;

import org.openjdk.jmh.annotations.Benchmark;

public class SimpleBenchmark extends BaseBenchmark
{
    @Benchmark
    public void myBenchmark() throws InterruptedException
    {
        Thread.sleep( 1 );
    }

    @Override
    public String description()
    {
        return "simple";
    }

    @Override
    public String benchmarkGroup()
    {
        return "test";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }
}
