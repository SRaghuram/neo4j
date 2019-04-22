/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Parameters;
import com.neo4j.bench.client.process.ProcessWrapper;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JvmVersion;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.client.results.RunPhase.MEASUREMENT;

public class VmStatTracer implements ExternalProfiler
{
    private ProcessWrapper vmstat;

    @Override
    public List<String> invokeArgs( ForkDirectory forkDirectory,
                                    BenchmarkGroup benchmarkGroup,
                                    Benchmark benchmark,
                                    Parameters additionalParameters )
    {
        return Collections.emptyList();
    }

    @Override
    public List<String> jvmArgs( JvmVersion jvmVersion,
                                 ForkDirectory forkDirectory,
                                 BenchmarkGroup benchmarkGroup,
                                 Benchmark benchmark,
                                 Parameters additionalParameters )
    {
        return Collections.emptyList();
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               BenchmarkGroup benchmarkGroup,
                               Benchmark benchmark,
                               Parameters additionalParameters )
    {
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                              benchmark,
                                                                                              MEASUREMENT,
                                                                                              ProfilerType.VM_STAT,
                                                                                              additionalParameters );

        Path vmstatLog = forkDirectory.pathFor( recordingDescriptor.sanitizedFilename( RecordingType.TRACE_VMSTAT ) );
        vmstat = ProcessWrapper.start( new ProcessBuilder()
                                               .command( "vmstat", "2", "-t", "-w", "-S", "M" )
                                               .redirectOutput( vmstatLog.toFile() )
                                               .redirectError( Redirect.INHERIT ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              BenchmarkGroup benchmarkGroup,
                              Benchmark benchmark,
                              Parameters additionalParameters )
    {
        vmstat.stop();
    }
}
