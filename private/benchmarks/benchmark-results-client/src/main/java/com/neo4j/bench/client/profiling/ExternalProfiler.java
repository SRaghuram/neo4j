package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JvmVersion;

import java.util.List;

public interface ExternalProfiler extends Profiler
{
    List<String> jvmInvokeArgs( ForkDirectory forkDirectory,
                                BenchmarkGroup benchmarkGroup,
                                Benchmark benchmark );

    List<String> jvmArgs( JvmVersion jvmVersion,
                          ForkDirectory forkDirectory,
                          BenchmarkGroup benchmarkGroup,
                          Benchmark benchmark );

    /**
     * Called before fork process is created.
     *
     * @param forkDirectory
     * @param benchmarkGroup
     * @param benchmark
     */
    void beforeProcess( ForkDirectory forkDirectory,
                        BenchmarkGroup benchmarkGroup,
                        Benchmark benchmark );

    /**
     * Called after fork process has terminated.
     * <p>
     * Perform cleanup tasks in here, e.g., renaming log files.
     *
     * @param forkDirectory
     * @param benchmarkGroup
     * @param benchmark
     */
    void afterProcess( ForkDirectory forkDirectory,
                       BenchmarkGroup benchmarkGroup,
                       Benchmark benchmark );
}
