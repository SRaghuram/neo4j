/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.benchmarks.executor;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.RunnerParams;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.infra.BenchmarkParams;

import static org.openjdk.jmh.annotations.Mode.SampleTime;

@BenchmarkEnabled( true )
public class FailingForSingleThreadBenchmark extends BaseBenchmark
{

    @Override
    protected void onSetup( BenchmarkGroup group,
                            com.neo4j.bench.model.model.Benchmark benchmark,
                            RunnerParams runnerParams,
                            BenchmarkParams benchmarkParams,
                            ForkDirectory forkDirectory )
    {
        if ( benchmarkParams.getThreads() == 1 )
        {
            throw new RuntimeException( "Test exception" );
        }
    }

    @Override
    public String benchmarkGroup()
    {
        return "Example";
    }

    @Override
    public String description()
    {
        return getClass().getSimpleName();
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Benchmark
    @BenchmarkMode( {SampleTime} )
    public void method()
    {
    }
}
