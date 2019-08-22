/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Profilers implementing this interface will be invoked periodically, during benchmark run.
 * By default, every 5 seconds. This can by change. You can add {@link FixedRate} annotation
 * to overwritten {@link ScheduledProfiler#onSchedule(Tick, ForkDirectory, BenchmarkGroup, Benchmark, Parameters, Jvm, Pid)} method.
 *
 */
public interface ScheduledProfiler extends ExternalProfiler
{
    @Retention( RetentionPolicy.RUNTIME )
    @Target( ElementType.METHOD )
    @interface FixedRate
    {
        int period() default 5;

        TimeUnit timeUnit() default TimeUnit.SECONDS;
    }

    /**
     * Called at fixed rate to sample profiler state. Default rate is every 5 seconds,
     * but it can be controlled by {@link FixedRate} annotation.
     * <br/>
     * <strong>IMPLEMENTORS NOTE:</strong>
     * <br/>
     * Remember that this method can be executed when process is already stopped
     * (during benchmark shutdown). It is implementor's responsibility to handle
     * such cases, gracefully.
     *
     * @param tick incremented with every invocation of scheduler profiler
     * @param forkDirectory forkDirectory directory to write files into
     * @param benchmarkGroup benchmark group
     * @param benchmark benchmark
     * @param additionalParameters additional parameters
     * @param jvm Java to use
     * @param pid ID of the process to be profiled
     */
    @FixedRate
    void onSchedule(
            Tick tick,
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters,
            Jvm jvm,
            Pid pid );
}
