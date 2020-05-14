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
import com.neo4j.bench.common.results.RunPhase;

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
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs(
            JvmVersion jvmVersion,
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters,
            Resources resources )
    {
        return JvmArgs.empty();
    }

    @Override
    public void beforeProcess(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
    }

    @Override
    public void afterProcess(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                benchmarkGroup,
                benchmark,
                RunPhase.MEASUREMENT,
                ProfilerType.NO_OP,
                additionalParameters );
        try
        {
            Files.createFile( forkDirectory.pathFor( recordingDescriptor ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to create empty file", e );
        }
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               BenchmarkGroup benchmarkGroup,
                               Benchmark benchmark,
                               Parameters additionalParameters )
    {
    }
}
