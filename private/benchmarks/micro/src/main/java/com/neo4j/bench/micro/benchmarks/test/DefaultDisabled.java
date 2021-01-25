/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.test;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@BenchmarkEnabled( false )
public class DefaultDisabled extends BaseRegularBenchmark
{
    @ParamValues(
            allowed = {"1"},
            base = {"1"} )
    @Param( {} )
    public int increment;

    @Override
    public String description()
    {
        return "Disabled by default";
    }

    @Override
    public String benchmarkGroup()
    {
        return "TestOnly";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @State( Scope.Thread )
    public static class TxState
    {
        long count;
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public long method( TxState txState )
    {
        return txState.count += increment;
    }
}
