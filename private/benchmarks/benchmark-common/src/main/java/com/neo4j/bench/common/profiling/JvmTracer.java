/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;
import static java.lang.String.format;

public class JvmTracer implements ExternalProfiler
{
    @Override
    public List<String> invokeArgs( ForkDirectory forkDirectory,
                                    BenchmarkGroup benchmarkGroup,
                                    Benchmark benchmark,
                                    Parameters additionalParameters )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs( JvmVersion jvmVersion,
                            ForkDirectory forkDirectory,
                            BenchmarkGroup benchmarkGroup,
                            Benchmark benchmark,
                            Parameters additionalParameters,
                            Resources resources )
    {
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                              benchmark,
                                                                                              MEASUREMENT,
                                                                                              ProfilerType.JVM_LOGGING,
                                                                                              additionalParameters );

        Path safepointLogs = forkDirectory.create( recordingDescriptor.sanitizedName() + ".safepoint.log" );
        Path vmLog = forkDirectory.pathFor( recordingDescriptor );
        return JvmArgs.from(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+CITime",
                format( "-Xlog:safepoint*=debug:file=%s", safepointLogs.toAbsolutePath().toString() ),
                "-XX:+LogVMOutput",
                format( "-XX:LogFile=%s", vmLog.toAbsolutePath().toString() ) );
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               BenchmarkGroup benchmarkGroup,
                               Benchmark benchmark,
                               Parameters additionalParameters )
    {
        // do nothing
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              BenchmarkGroup benchmarkGroup,
                              Benchmark benchmark,
                              Parameters additionalParameters )
    {
        // do nothing
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                               Parameters additionalParameters )
    {
        // do nothing
    }
}
