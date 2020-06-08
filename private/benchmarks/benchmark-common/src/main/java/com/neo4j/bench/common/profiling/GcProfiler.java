/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.GC;
import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.common.util.BenchmarkUtil.appendFile;
import static java.lang.String.format;

public class GcProfiler implements ExternalProfiler
{
    // profiler log -- used by this class only
    private static String gcProfilerLogName( Parameters parameters )
    {
        String additionalParametersString = parameters.isEmpty() ? "" : "-" + parameters.toString();
        return "gc-profiler" + additionalParametersString + ".log";
    }

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
                                                                                              GC,
                                                                                              additionalParameters );
        Path gcLog = forkDirectory.pathFor( recordingDescriptor );
        List<String> gcLogJvmArgs = jvmLogArgs( jvmVersion, gcLog.toAbsolutePath().toString() );

        // profiler log -- used by this class only
        Path profilerLog = forkDirectory.create( gcProfilerLogName( additionalParameters ) );
        appendFile( profilerLog, Instant.now(), "Added GC specific JVM args: " + gcLogJvmArgs );
        return JvmArgs.from( gcLogJvmArgs );
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
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                              benchmark,
                                                                                              MEASUREMENT,
                                                                                              GC,
                                                                                              additionalParameters );

        // profiler log -- used by this class only
        Path profilerLog = forkDirectory.findOrCreate( gcProfilerLogName( additionalParameters ) );

        Path gcLogFile = forkDirectory.pathFor( recordingDescriptor );

        try
        {
            appendFile( profilerLog,
                        Instant.now(),
                        "Parsing GC log file: " + gcLogFile.toAbsolutePath() );

            GcLog gcLog = GcLog.parse( gcLogFile );

            Path gcLogJson = forkDirectory.pathFor( recordingDescriptor.sanitizedFilename( RecordingType.GC_SUMMARY ) );
            Path gcLogCsv = forkDirectory.pathFor( recordingDescriptor.sanitizedFilename( RecordingType.GC_CSV ) );

            appendFile( profilerLog,
                        Instant.now(),
                        "Exporting GC log data to JSON: " + gcLogJson.toAbsolutePath() );

            JsonUtil.serializeJson( gcLogJson, gcLog );

            appendFile( profilerLog,
                        Instant.now(),
                        "Exporting GC log data to CSV: " + gcLogCsv.toAbsolutePath() );

            gcLog.toCSV( gcLogCsv );

            appendFile( profilerLog,
                        Instant.now(),
                        "Successfully processed GC log" );
        }
        catch ( IOException e )
        {
            appendFile( profilerLog,
                        Instant.now(),
                        "Error processing GC log file: " + gcLogFile.toAbsolutePath() );
            throw new RuntimeException( "Error processing GC log file: " + gcLogFile.toAbsolutePath(), e );
        }
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                               Parameters additionalParameters )
    {
        // do nothing
    }

    private static List<String> jvmLogArgs( JvmVersion jvmVersion, String sanitizedGcLogFilename )
    {
        if ( jvmVersion.majorVersion() <= 8 )
        {
            return Lists.newArrayList(
                    "-XX:+PrintGC", // Print basic GC info
                    "-XX:+PrintGCDetails", // Print more elaborated GC info
                    "-XX:+PrintGCDateStamps", // Print date stamps at garbage collection events (e.g. 2011-09-08T14:20:29.557+0400: [GC... )
                    "-XX:+PrintGCApplicationStoppedTime", // Print pause summary after each stop-the-world pause
                    "-XX:+PrintGCApplicationConcurrentTime", // Print time for each concurrent phase of GC
                    "-XX:+PrintTenuringDistribution", // Print detailed demography of young space after each collection
                    "-Xloggc:" + sanitizedGcLogFilename // Redirects GC output to a file instead of console
            );
        }
        else
        {
            return Lists.newArrayList( format(
                    "-Xlog:gc," + // Print basic GC info
                    "safepoint," + // Print safepoints (application stopped time)
                    "gc+age=trace" + // Print tenuring distribution
                    ":file=%s:tags,time,uptime,level", // Print logs level, timestamp and VM uptime decorations
                    sanitizedGcLogFilename ) );
        }
    }
}
