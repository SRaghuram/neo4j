/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.results.ForkDirectory;

import java.util.List;

public interface ExternalProfiler extends Profiler
{
    /**
     * Returns any additional command line arguments, that will prefix the normal command used to launch the process that is being benchmarked.
     *
     * @param forkDirectory directory to write files into
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     * @return command line prefix
     */
    List<String> invokeArgs( ForkDirectory forkDirectory,
                             BenchmarkGroup benchmarkGroup,
                             Benchmark benchmark,
                             Parameters additionalParameters );

    /**
     * Returns additional JVM arguments for launching a Java process.
     * This is additive, i.e., the returned arguments will be added to the list of JVM arguments that would otherwise already be used.
     *
     * @param jvmVersion the version of JVM that was used to launch the benchmarked process
     * @param forkDirectory directory to write files into
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     * @param resources from JAR files
     * @return additional JVM arguments
     */
    JvmArgs jvmArgs( JvmVersion jvmVersion,
                     ForkDirectory forkDirectory,
                     BenchmarkGroup benchmarkGroup,
                     Benchmark benchmark,
                     Parameters additionalParameters,
                     Resources resources );

    /**
     * Will be called before benchmark process is launched.
     * Any initializing/starting of the profiler should be done here before returning.
     * This method must be non-blocking, i.e., should start a profiler that runs concurrently with the profiled process.
     *
     * @param forkDirectory directory to write files into
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void beforeProcess( ForkDirectory forkDirectory,
                        BenchmarkGroup benchmarkGroup,
                        Benchmark benchmark,
                        Parameters additionalParameters );

    /**
     * Will be called after benchmark process terminates.
     * Any stopping/dumping related to the profiler should be done here before returning.
     *
     * @param forkDirectory directory to write files into
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void afterProcess( ForkDirectory forkDirectory,
                       BenchmarkGroup benchmarkGroup,
                       Benchmark benchmark,
                       Parameters additionalParameters );

    /**
     * Will be called after benchmark process terminates, if the process fails.
     * Any stopping/dumping related to the profiler should be done here before returning.
     *
     * @param forkDirectory directory to write files into
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters, used to distinguish processes when multiple processes are involved in executing the same benchmark
     */
    void processFailed( ForkDirectory forkDirectory,
                        BenchmarkGroup benchmarkGroup,
                        Benchmark benchmark,
                        Parameters additionalParameters );
}
