/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.GC;
import static com.neo4j.bench.common.util.BenchmarkUtil.appendFile;
import static java.lang.String.format;

public class GcProfiler implements ExternalProfiler
{
    // profiler log -- used by this class only
    private static String gcProfilerLogName( RecordingDescriptor recordingDescriptor )
    {
        return recordingDescriptor.sanitizedFilename( "gc-profiler-", ".log" );
    }

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
        RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.GC_LOG );
        Path gcLogFile = forkDirectory.registerPathFor( recordingDescriptor );
        List<String> gcLogJvmArgs = jvmLogArgs( jvmVersion, gcLogFile.toAbsolutePath().toString() );

        // profiler log -- used by this class only
        Path profilerLog = forkDirectory.create( gcProfilerLogName( recordingDescriptor ) );
        appendFile( profilerLog, Instant.now(), "Added GC specific JVM args: " + gcLogJvmArgs );
        return JvmArgs.from( gcLogJvmArgs );
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
        GC.maybeSecondaryRecordingCreator()
          .ifPresent( secondaryRecordingCreator -> secondaryRecordingCreator.create( profilerRecordingDescriptor, forkDirectory ) );
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
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
