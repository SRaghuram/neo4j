/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.test_only;

import com.neo4j.bench.micro.benchmarks.core.AbstractCoreBenchmark;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Param;

import static org.openjdk.jmh.annotations.Mode.SampleTime;

@BenchmarkEnabled( false )
public class ValidDisabledBenchmark extends AbstractCoreBenchmark
{
    @ParamValues(
            allowed = {"1", "2"},
            base = {"1"} )
    @Param( {} )
    public int ValidDisabledBenchmark_param1;

    @ParamValues(
            allowed = {"a", "b"},
            base = {"a"} )
    @Param( {} )
    public String ValidDisabledBenchmark_param2;

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
