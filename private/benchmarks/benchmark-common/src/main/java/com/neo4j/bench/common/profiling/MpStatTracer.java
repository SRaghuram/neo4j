/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.results.ForkDirectory;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;

public class MpStatTracer implements ExternalProfiler
{
    private ProcessWrapper mpstat;

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
        return JvmArgs.empty();
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
                                                                                              ProfilerType.MP_STAT,
                                                                                              additionalParameters );

        Path mpstatLog = forkDirectory.pathFor( recordingDescriptor );
        mpstat = ProcessWrapper.start( new ProcessBuilder()
                                               .command( "mpstat", "2", "-P", "ALL" )
                                               .redirectOutput( mpstatLog.toFile() )
                                               .redirectError( Redirect.INHERIT ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              BenchmarkGroup benchmarkGroup,
                              Benchmark benchmark,
                              Parameters additionalParameters )
    {
        mpstat.stop();
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                               Parameters additionalParameters )
    {
        // do nothing
    }
}
