/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JvmVersion;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.client.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.client.util.BenchmarkUtil.sanitize;

public class JvmTracer implements ExternalProfiler
{
    @Override
    public List<String> jvmInvokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Collections.emptyList();
    }

    @Override
    public List<String> jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        ProfilerRecordingDescriptor recordingDescriptor = new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, ProfilerType.JVM_LOGGING );

        Path heapDump = forkDirectory.create( recordingDescriptor.name() + ".hprof" );
        Path vmLog = forkDirectory.pathFor( recordingDescriptor );
        return Lists.newArrayList(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+HeapDumpOnOutOfMemoryError",
                "-XX:HeapDumpPath=" + sanitize( heapDump.toAbsolutePath().toString() ),
                "-XX:+CITime",
                "-XX:+PrintSafepointStatistics",
                "-XX:PrintSafepointStatisticsCount=1",
                "-XX:PrintSafepointStatisticsTimeout=500",
//                "-XX:+TraceSafepointCleanupTime", // TODO temporarily remove as it appears in system out
                "-XX:+LogVMOutput",
                "-XX:LogFile=" + vmLog.toAbsolutePath().toString() );
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        // do nothing
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        // do nothing
    }
}
