/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;

public interface InternalProfiler extends Profiler
{
    /**
     * This method must be non-blocking
     *
     * @param forkDirectory
     * @param pid
     * @param benchmark
     */
    void onWarmupBegin( Jvm jvm,
                        ForkDirectory forkDirectory,
                        long pid,
                        BenchmarkGroup benchmarkGroup,
                        Benchmark benchmark );

    /**
     * This method must be non-blocking
     *
     * @param forkDirectory
     * @param pid
     * @param benchmark
     */
    void onWarmupFinished( Jvm jvm,
                           ForkDirectory forkDirectory,
                           long pid,
                           BenchmarkGroup benchmarkGroup,
                           Benchmark benchmark );

    /**
     * This method must be non-blocking
     *
     * @param forkDirectory
     * @param pid
     * @param benchmark
     */
    void onMeasurementBegin( Jvm jvm,
                             ForkDirectory forkDirectory,
                             long pid,
                             BenchmarkGroup benchmarkGroup,
                             Benchmark benchmark );

    /**
     * This method must be non-blocking
     *
     * @param forkDirectory
     * @param pid
     * @param benchmark
     */
    void onMeasurementFinished( Jvm jvm,
                                ForkDirectory forkDirectory,
                                long pid,
                                BenchmarkGroup benchmarkGroup,
                                Benchmark benchmark );
}
