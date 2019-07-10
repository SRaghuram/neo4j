/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.nmt;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.RecordingType;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor.ParseResult;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static com.neo4j.bench.common.profiling.NativeMemoryTrackingSummaryProfiler.SNAPSHOT_PARAM;

import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class NativeMemoryTrackingSummaryReport
{

    private List<NativeMemoryTrackingSummary> snapshots;

    public NativeMemoryTrackingSummaryReport( List<NativeMemoryTrackingSummary> snapshots )
    {
        this.snapshots = snapshots;
    }

    public static NativeMemoryTrackingSummaryReport create(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            RunPhase runPhase ) throws IOException
    {

        // list NMT summary files, sort by snapshot

        NativeMemoryTrackingSummaryParser parser = new NativeMemoryTrackingSummaryParser();

        try ( Stream<Path> files = Files.find(
                Paths.get( forkDirectory.toAbsolutePath() ),
                1,
                ( path, attrs ) -> path.getFileName().toString().endsWith( RecordingType.NMT_SUMMARY.extension() ) ) )
        {

            List<NativeMemoryTrackingSummary> snapshots = files.map( path -> {
                ParseResult parseResult = ProfilerRecordingDescriptor.tryParse(
                        path.getFileName().toString(),
                        RecordingType.NMT_SUMMARY,
                        benchmarkGroup,
                        benchmark,
                        runPhase );
                return new RecordingSnapshot( path, parseResult.additionalParameters() );
            } )
            .sorted( comparingLong( k -> Long.parseLong( k.parameters.asMap().get( SNAPSHOT_PARAM ) ) ) )
            .map( r -> r.recordingPath )
            .map( parser::parse )
            .collect( toList() );

            return new NativeMemoryTrackingSummaryReport( snapshots );
        }
    }

    public long[] getReservedKBInCategory( String category )
    {
        return snapshots
                .stream()
                .map( k -> k.getCategory( category ) )
                .mapToLong( NativeMemoryTrackingCategory::getReserved )
                .toArray();
    }

    public long[] getCommittedKBInCategory( String category )
    {
        return snapshots
                .stream()
                .map( k -> k.getCategory( category ) )
                .mapToLong( NativeMemoryTrackingCategory::getCommitted )
                .toArray();
    }

    public void toCSV( Path path ) throws IOException
    {
        List<String> categories = snapshots.stream()
                .flatMap( k -> k.getCategories().stream() )
                .distinct()
                .collect( toList() );

        String delimiter = ",";
        String headers = categories.stream()
                .flatMap( k -> Stream.of( format( "%s (reserved)", k),  format( "%s (committed)", k) ) )
                .collect( joining( delimiter ) );

        try ( PrintWriter writer = new PrintWriter(
                Files.newBufferedWriter( path, StandardOpenOption.CREATE, StandardOpenOption.WRITE ) ) )
        {
            writer.println( headers );
            snapshots.stream()
                    .map( k -> categories.stream()
                            .flatMap( category -> Stream.of(
                                    Long.toString( k.getCategory( category ).getReserved() ),
                                    Long.toString( k.getCategory( category ).getCommitted() ) ) )
                            .collect( joining( delimiter ) ) )
                    .forEach( writer::println );
        }
    }

    private static class RecordingSnapshot
    {
        final Path recordingPath;
        final Parameters parameters;

        RecordingSnapshot( Path recordingPath, Parameters parameters )
        {
            super();
            this.recordingPath = recordingPath;
            this.parameters = parameters;
        }

    }

}
