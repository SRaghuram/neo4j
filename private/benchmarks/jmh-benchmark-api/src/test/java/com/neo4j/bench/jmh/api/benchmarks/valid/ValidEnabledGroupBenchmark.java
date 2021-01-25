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
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Param;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

@BenchmarkEnabled( true )
public class ValidEnabledGroupBenchmark extends BaseBenchmark
{
    @ParamValues(
            allowed = {"a", "b"},
            base = {"a", "b"} )
    @Param( {} )
    public String string;

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

    @Group( "group" )
    @Benchmark
    @BenchmarkMode( {AverageTime} )
    public void methodOne()
    {
    }

    @Group( "group" )
    @Benchmark
    @BenchmarkMode( {AverageTime} )
    public void methodTwo()
    {
    }
}
