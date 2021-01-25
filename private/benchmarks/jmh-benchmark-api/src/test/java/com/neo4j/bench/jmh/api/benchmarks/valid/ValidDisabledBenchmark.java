/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.benchmarks.valid;

import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Param;

import static org.openjdk.jmh.annotations.Mode.SampleTime;

@BenchmarkEnabled( false )
public class ValidDisabledBenchmark extends BaseBenchmark
{
    @ParamValues(
            allowed = {"1", "2"},
            base = {"1"} )
    @Param( {} )
    public int param1;

    @ParamValues(
            allowed = {"a", "b"},
            base = {"a"} )
    @Param( {} )
    public String param2;

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
        return false;
    }

    @Benchmark
    @BenchmarkMode( {SampleTime} )
    public void method()
    {
    }
}
