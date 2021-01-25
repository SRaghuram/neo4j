/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.jmh.api.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.infra.Blackhole;

import java.util.stream.IntStream;

public class SimpleBenchmark extends BaseSimpleBenchmark
{
    @ParamValues(
            allowed = {"10000", "20000"},
            base = {"10000", "20000"}
    )
    @Param( {} )
    public Integer range;

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

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public void count( Blackhole bh )
    {
        bh.consume( IntStream.range( 0, range ).map( i -> i * i ).count() );
    }

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public void spin( Blackhole bh )
    {
        for ( int i = 0; i < range; i++ )
        {
            bh.consume( i );
        }
    }
}
