/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.profiling.RecordingType;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class MpStatTracer implements ExternalProfiler
{
    private ProcessWrapper mpstat;

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
        return JvmArgs.empty();
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        Path mpstatLog = forkDirectory.registerPathFor( profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.TRACE_MPSTAT ) );
        mpstat = ProcessWrapper.start( new ProcessBuilder()
                                               .command( "mpstat", "2", "-P", "ALL" )
                                               .redirectOutput( mpstatLog.toFile() )
                                               .redirectError( Redirect.INHERIT ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        mpstat.stop();
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }
}
