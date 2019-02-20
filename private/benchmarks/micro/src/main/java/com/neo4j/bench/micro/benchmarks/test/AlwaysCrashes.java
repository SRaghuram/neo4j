package com.neo4j.bench.micro.benchmarks.test;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

@BenchmarkEnabled( false )
public class AlwaysCrashes extends BaseDatabaseBenchmark
{
    @Override
    public String description()
    {
        return "Throws an exception on every invocation";
    }

    @Override
    public String benchmarkGroup()
    {
        return "TestOnly";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .build();
    }

    @Benchmark
    @BenchmarkMode( {Mode.AverageTime} )
    public long method()
    {
        throw new Kaboom( "This benchmark is supposed to always crash. This is no surprise." );
    }
}
