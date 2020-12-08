/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.google.common.collect.Sets;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.appendIfMissing;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class ResultsCopy
{
    private static final Set<RecordingType> IGNORED_RECORDING_TYPES = Sets.newHashSet( RecordingType.NONE,
                                                                                       RecordingType.HEAP_DUMP,
                                                                                       RecordingType.TRACE_STRACE,
                                                                                       RecordingType.TRACE_MPSTAT,
                                                                                       RecordingType.TRACE_VMSTAT,
                                                                                       RecordingType.TRACE_IOSTAT,
                                                                                       RecordingType.TRACE_JVM );

    /**
     * This method will:
     *  <ol>
     *   <li>Discover profiler recordings in `workDir`</li>
     *   <li>Attach each discovered recording to the provided {@link TestRunReport}</li>
     *   <li>Copy each discovered recording to `tempProfilerRecordingsDir`</li>
     * </ol>
     */
    static void extractProfilerRecordings( BenchmarkGroupBenchmarkMetrics metrics, Path tempProfilerRecordingsDir, URI s3FolderUri, Path workDir )
    {
        String s3Folder = appendIfMissing( removeStart( s3FolderUri.toString(), "s3://" ), "/" );

        for ( BenchmarkGroupDirectory benchmarkGroupDirectory : BenchmarkGroupDirectory.searchAllIn( workDir ) )
        {
            for ( BenchmarkDirectory benchmarksDirectory : benchmarkGroupDirectory.benchmarkDirectories() )
            {
                BenchmarkGroupBenchmark benchmarkGroupBenchmark = new BenchmarkGroupBenchmark( benchmarkGroupDirectory.benchmarkGroup(),
                                                                                               benchmarksDirectory.benchmark() );
                // Only process successful benchmarks
                if ( metrics.benchmarkGroupBenchmarks().contains( benchmarkGroupBenchmark ) )
                {
                    ProfilerRecordings profilerRecordings = new ProfilerRecordings();

                    copyValidRecordings( benchmarksDirectory, tempProfilerRecordingsDir )
                            .forEach( ( recordingDescriptor, path ) ->
                                              // attached valid/copied recordings to test run report
                                              profilerRecordings.with( recordingDescriptor.recordingType(),
                                                                       recordingDescriptor.additionalParams(),
                                                                       s3Folder + path.toString() )
                            );

                    if ( !profilerRecordings.toMap().isEmpty() )
                    {
                        // TODO once we have parameterized profilers we should assert that every expected recording exists
                        metrics
                                .attachProfilerRecording( benchmarkGroupBenchmark.benchmarkGroup(),
                                                          benchmarkGroupBenchmark.benchmark(),
                                                          profilerRecordings );
                    }
                }
            }
        }
    }

    private static Map<RecordingDescriptor,Path> copyValidRecordings( BenchmarkDirectory benchmarksDirectory, Path tempProfilerRecordingsDir )
    {
        return benchmarksDirectory.forks().stream()
                                  .flatMap( forkDirectory -> forkDirectory.copyProfilerRecordings( tempProfilerRecordingsDir, ResultsCopy::shouldUpload )
                                                                          .entrySet()
                                                                          .stream() )
                                  .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    private static boolean shouldUpload( RecordingDescriptor recordingDescriptor )
    {
        return !IGNORED_RECORDING_TYPES.contains( recordingDescriptor.recordingType() );
    }
}
