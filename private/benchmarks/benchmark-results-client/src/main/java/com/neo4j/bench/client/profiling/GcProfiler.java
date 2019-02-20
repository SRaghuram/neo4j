/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.client.util.JvmVersion;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.client.profiling.ProfilerType.GC;
import static com.neo4j.bench.client.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.client.util.BenchmarkUtil.appendFile;

import static java.lang.String.format;

public class GcProfiler implements ExternalProfiler
{
    private static final String GC_PROFILER_LOG = "gc-profiler.log";

    @Override
    public List<String> jvmInvokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Collections.emptyList();
    }

    @Override
    public List<String> jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        ProfilerRecordingDescriptor recordingDescriptor = new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, GC );
        Path gcLog = forkDirectory.pathFor( recordingDescriptor );
        List<String> gcLogJvmArgs = jvmLogArgs( jvmVersion, gcLog.toAbsolutePath().toString() );

        // profiler log -- used by this class only
        Path profilerLog = forkDirectory.create( GC_PROFILER_LOG );
        appendFile( profilerLog, Instant.now(), "Added GC specific JVM args: " + gcLogJvmArgs );
        return gcLogJvmArgs;
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        // do nothing
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        ProfilerRecordingDescriptor recordingDescriptor = new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, GC );

        // profiler log -- used by this class only
        Path profilerLog = forkDirectory.findOrCreate( GC_PROFILER_LOG );

        Path gcLogFile = forkDirectory.pathFor( recordingDescriptor );

        try
        {
            appendFile( profilerLog,
                        Instant.now(),
                        "Parsing GC log file: " + gcLogFile.toAbsolutePath() );

            GcLog gcLog = GcLog.parse( gcLogFile );

            Path gcLogJson = forkDirectory.pathFor( recordingDescriptor.filename( RecordingType.GC_SUMMARY ) );
            Path gcLogCsv = forkDirectory.pathFor( recordingDescriptor.filename( RecordingType.GC_CSV ) );

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
                    "-Xlog:gc,"    + // Print basic GC info
                    "safepoint,"   + // Print safepoints (application stopped time)
                    "gc+age=trace" + // Print tenuring distribution
                    ":file=%s:tags,time,uptime,level", // Print logs level, timestamp and VM uptime decorations
                    sanitizedGcLogFilename ) );
        }
    }
}
