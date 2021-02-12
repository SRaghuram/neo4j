/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.google.common.collect.Sets;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class ResultsCopy
{
    private static final Set<RecordingType> IGNORED_RECORDING_TYPES = Sets.newHashSet( RecordingType.NONE,
                                                                                       RecordingType.HEAP_DUMP,
                                                                                       RecordingType.TRACE_STRACE,
                                                                                       RecordingType.TRACE_MPSTAT,
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
    static void extractProfilerRecordings( BenchmarkGroupBenchmarkMetrics metrics,
                                           Path tempProfilerRecordingsDir,
                                           String s3Folder,
                                           Path workDir )
    {
        Set<FullBenchmarkName> successfulBenchmarks = extractSuccessfulBenchmarks( metrics );

        for ( BenchmarkGroupDirectory benchmarkGroupDirectory : BenchmarkGroupDirectory.searchAllIn( workDir ) )
        {
            for ( BenchmarkDirectory benchmarksDirectory : benchmarkGroupDirectory.benchmarkDirectories() )
            {
                Map<FullBenchmarkName,ProfilerRecordings> benchmarkRecordings = new HashMap<>();
                copyValidRecordings( benchmarksDirectory, tempProfilerRecordingsDir, successfulBenchmarks )
                        .forEach( ( recordingDescriptor, path ) ->
                                          benchmarkRecordings.computeIfAbsent( recordingDescriptor.benchmarkName(), key -> new ProfilerRecordings() )
                                                             // attach valid/copied recordings to test run report
                                                             .with( recordingDescriptor.recordingType(),
                                                                    recordingDescriptor.additionalParams(),
                                                                    s3Folder + path.toString() )
                        );

                benchmarkRecordings
                        .forEach( ( fullBenchmarkName, profilerRecordings ) ->
                                          // TODO once we have parameterized profilers we should assert that every expected recording exists
                                          metrics.attachProfilerRecording( fullBenchmarkName.benchmarkGroup(),
                                                                           fullBenchmarkName.benchmark(),
                                                                           profilerRecordings ) );
            }
        }
    }

    private static Set<FullBenchmarkName> extractSuccessfulBenchmarks( BenchmarkGroupBenchmarkMetrics metrics )
    {
        return metrics.benchmarkGroupBenchmarks()
                      .stream()
                      .map( benchmarkGroupBenchmark -> FullBenchmarkName.from( benchmarkGroupBenchmark.benchmarkGroup(), benchmarkGroupBenchmark.benchmark() ) )
                      .collect( Collectors.toSet() );
    }

    private static Map<RecordingDescriptor,Path> copyValidRecordings( BenchmarkDirectory benchmarksDirectory,
                                                                      Path tempProfilerRecordingsDir,
                                                                      Set<FullBenchmarkName> successfulBenchmarks )
    {
        Map<RecordingDescriptor,Path> recordingsMap = new HashMap<>();

        benchmarksDirectory
                .forks()
                .stream()
                .map( forkDirectory -> copyValidRecordings( forkDirectory, tempProfilerRecordingsDir, successfulBenchmarks ) )
                .flatMap( map -> map.entrySet().stream() )
                .forEach( entry ->
                          {
                              RecordingDescriptor recordingDescriptor = entry.getKey();
                              Path recordingFile = entry.getValue();
                              if ( !recordingsMap.containsKey( recordingDescriptor ) || recordingDescriptor.isDuplicatesAllowed() )
                              {
                                  recordingsMap.put( recordingDescriptor, recordingFile );
                              }
                              else
                              {
                                  throw new IllegalStateException( format( "Duplicate recording descriptor found%n" +
                                                                           "Recording Descriptor: %s%n" +
                                                                           "Recording File:        %s",
                                                                           recordingDescriptor, recordingFile ) );
                              }
                          } );
        return recordingsMap;
    }

    private static Map<RecordingDescriptor,Path> copyValidRecordings( ForkDirectory forkDirectory,
                                                                      Path tempProfilerRecordingsDir,
                                                                      Set<FullBenchmarkName> successfulBenchmarks )
    {
        Map<RecordingDescriptor,Path> descriptorToPath = forkDirectory
                .copyProfilerRecordings( tempProfilerRecordingsDir, descriptor -> shouldUpload( descriptor, successfulBenchmarks ) );

        return descriptorToPath
                .entrySet()
                .stream()
                .map( entry -> expandSuccessfulDescriptors( entry.getKey(), entry.getValue(), successfulBenchmarks ) )
                .flatMap( map -> map.entrySet().stream() )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    private static boolean shouldUpload( RecordingDescriptor recordingDescriptor,
                                         Set<FullBenchmarkName> successfulBenchmarks )
    {
        return !IGNORED_RECORDING_TYPES.contains( recordingDescriptor.recordingType() ) &&
               !findSuccessfulDescriptors( successfulBenchmarks, recordingDescriptor ).isEmpty();
    }

    private static Map<RecordingDescriptor,Path> expandSuccessfulDescriptors( RecordingDescriptor recordingDescriptor,
                                                                              Path path,
                                                                              Set<FullBenchmarkName> successfulBenchmarks )
    {
        return findSuccessfulDescriptors( successfulBenchmarks, recordingDescriptor )
                .stream()
                .collect( Collectors.toMap( Function.identity(), descriptor -> path ) );
    }

    private static Set<RecordingDescriptor> findSuccessfulDescriptors( Set<FullBenchmarkName> successfulBenchmarks,
                                                                       RecordingDescriptor recordingDescriptor )
    {
        Set<RecordingDescriptor> descriptors = Sets.union( Collections.singleton( recordingDescriptor ), recordingDescriptor.secondaryRecordingDescriptors() );
        return descriptors.stream()
                          .filter( descriptor -> successfulBenchmarks.contains( descriptor.benchmarkName() ) )
                          .collect( Collectors.toSet() );
    }
}
