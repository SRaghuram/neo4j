/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.model.profiling.RecordingType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

/**
 * This profiler is only used for JMH benchmarks.
 * In standard JMH, a benchmark process has no way of knowing in which fork it is currently running.
 * To augment JMH with this feature, the JMH-wrapping benchmark suite relies on a mechanism that requires AT LEAST one profiler to always be configured.
 * That is the purpose of this profiler, i.e., every JMH benchmark process will ALWAYS run with this profiler.
 */
public class NoOpProfiler implements ExternalProfiler
{
    @Override
    public List<String> invokeArgs(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs(
            JvmVersion jvmVersion,
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return JvmArgs.empty();
    }

    @Override
    public void beforeProcess(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
    }

    @Override
    public void afterProcess(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        try
        {
            Files.createFile( forkDirectory.registerPathFor( profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.NONE ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to create empty file", e );
        }
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
    }
}
