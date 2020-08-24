/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

public class JvmTracer implements ExternalProfiler
{
    @Override
    public List<String> invokeArgs( ForkDirectory forkDirectory,
                                    ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs( JvmVersion jvmVersion,
                            ForkDirectory forkDirectory,
                            ProfilerRecordingDescriptor profilerRecordingDescriptor,
                            Resources resources )
    {
        RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.TRACE_JVM );
        Path safepointLogs = forkDirectory.create( recordingDescriptor.sanitizedName() + ".safepoint.log" );
        Path vmLog = forkDirectory.registerPathFor( recordingDescriptor );
        return JvmArgs.from(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+CITime",
                format( "-Xlog:safepoint*=debug:file=%s", safepointLogs.toAbsolutePath().toString() ),
                "-XX:+LogVMOutput",
                format( "-XX:LogFile=%s", vmLog.toAbsolutePath().toString() ) );
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }
}
