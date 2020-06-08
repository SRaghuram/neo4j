/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.common.results.ForkDirectory;

public interface InternalProfiler extends Profiler
{
    /**
     * Will be called immediately before benchmark warmup begins.
     * Any initializing/starting of the profiler should be done here before returning.
     * This method must be non-blocking, i.e., should start a profiler that runs concurrently with the profiled process.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void onWarmupBegin( Jvm jvm,
                        ForkDirectory forkDirectory,
                        Pid pid,
                        BenchmarkGroup benchmarkGroup,
                        Benchmark benchmark,
                        Parameters additionalParameters );

    /**
     * Will be called immediately after benchmark warmup finishes.
     * Any stopping/dumping related to the profiler should be done here before returning.
     * This method must be non-blocking.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void onWarmupFinished( Jvm jvm,
                           ForkDirectory forkDirectory,
                           Pid pid,
                           BenchmarkGroup benchmarkGroup,
                           Benchmark benchmark,
                           Parameters additionalParameters );

    /**
     * Will be called immediately before benchmark measurement begins.
     * Any initializing/starting of the profiler should be done here before returning.
     * This method must be non-blocking, i.e., should start a profiler that runs concurrently with the profiled process.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void onMeasurementBegin( Jvm jvm,
                             ForkDirectory forkDirectory,
                             Pid pid,
                             BenchmarkGroup benchmarkGroup,
                             Benchmark benchmark,
                             Parameters additionalParameters );

    /**
     * Will be called immediately after benchmark measurement finishes.
     * Any stopping/dumping related to the profiler should be done here before returning.
     * This method must be non-blocking.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void onMeasurementFinished( Jvm jvm,
                                ForkDirectory forkDirectory,
                                Pid pid,
                                BenchmarkGroup benchmarkGroup,
                                Benchmark benchmark,
                                Parameters additionalParameters );
}
