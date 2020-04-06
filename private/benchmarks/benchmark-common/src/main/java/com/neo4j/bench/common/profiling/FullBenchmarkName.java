/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;

import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;

public class FullBenchmarkName
{
    public static FullBenchmarkName from( BenchmarkGroupBenchmark benchmarkGroupBenchmark )
    {
        return from( benchmarkGroupBenchmark.benchmarkGroup(), benchmarkGroupBenchmark.benchmark() );
    }

    public static FullBenchmarkName from( BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return new FullBenchmarkName( benchmarkGroup, benchmark );
    }

    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;

    private FullBenchmarkName( BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        this.benchmarkGroup = benchmarkGroup;
        this.benchmark = benchmark;
    }

    public String sanitizedName()
    {
        return sanitize( name() );
    }

    public String name()
    {
        return benchmarkGroup.name() + "." + benchmark.name();
    }
}
