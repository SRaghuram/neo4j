/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;

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
     * @param profilerRecordingDescriptor contains the data and logic needed to create appropriate profiler recording files
     */
    void onWarmupBegin( Jvm jvm,
                        ForkDirectory forkDirectory,
                        Pid pid,
                        ProfilerRecordingDescriptor profilerRecordingDescriptor );

    /**
     * Will be called immediately after benchmark warmup finishes.
     * Any stopping/dumping related to the profiler should be done here before returning.
     * This method must be non-blocking.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param profilerRecordingDescriptor contains the data and logic needed to create appropriate profiler recording files
     */
    void onWarmupFinished( Jvm jvm,
                           ForkDirectory forkDirectory,
                           Pid pid,
                           ProfilerRecordingDescriptor profilerRecordingDescriptor );

    /**
     * Will be called immediately before benchmark measurement begins.
     * Any initializing/starting of the profiler should be done here before returning.
     * This method must be non-blocking, i.e., should start a profiler that runs concurrently with the profiled process.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param profilerRecordingDescriptor contains the data and logic needed to create appropriate profiler recording files
     */
    void onMeasurementBegin( Jvm jvm,
                             ForkDirectory forkDirectory,
                             Pid pid,
                             ProfilerRecordingDescriptor profilerRecordingDescriptor );

    /**
     * Will be called immediately after benchmark measurement finishes.
     * Any stopping/dumping related to the profiler should be done here before returning.
     * This method must be non-blocking.
     *
     * @param jvm Java to use
     * @param forkDirectory directory to write files into
     * @param pid ID of the process to be profiled
     * @param profilerRecordingDescriptor contains the data and logic needed to create appropriate profiler recording files
     */
    void onMeasurementFinished( Jvm jvm,
                                ForkDirectory forkDirectory,
                                Pid pid,
                                ProfilerRecordingDescriptor profilerRecordingDescriptor );
}
