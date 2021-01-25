/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class NativeMemoryTrackingSummaryReport
{

    private List<NativeMemoryTrackingSummary> snapshots;

    private NativeMemoryTrackingSummaryReport( List<NativeMemoryTrackingSummary> snapshots )
    {
        this.snapshots = snapshots;
    }

    public static NativeMemoryTrackingSummaryReport create( Path snapshotDir ) throws IOException
    {
        // list NMT summary files, sort by snapshot
        try ( Stream<Path> files = Files.find(
                snapshotDir,
                1,
                ( path, attrs ) -> NativeMemoryTrackingSnapshot.matches( path.getFileName().toString() ) ) )
        {

            List<NativeMemoryTrackingSummary> snapshots = files
            .sorted( comparingLong( path -> NativeMemoryTrackingSnapshot.counter( path.getFileName().toString() ) ) )
            .map( NativeMemoryTrackingSummaryParser::parse )
            .collect( toList() );

            return new NativeMemoryTrackingSummaryReport( snapshots );
        }
    }

    long[] getReservedKBInCategory( String category )
    {
        return snapshots
                .stream()
                .map( summary -> summary.getCategory( category ) )
                .mapToLong( NativeMemoryTrackingCategory::getReserved )
                .toArray();
    }

    long[] getCommittedKBInCategory( String category )
    {
        return snapshots
                .stream()
                .map( summary -> summary.getCategory( category ) )
                .mapToLong( NativeMemoryTrackingCategory::getCommitted )
                .toArray();
    }

    public void toCSV( Path path ) throws IOException
    {
        List<String> categories = snapshots.stream()
                .flatMap( summary -> summary.getCategories().stream() )
                .distinct()
                .collect( toList() );

        String delimiter = ",";
        String headers = categories.stream()
                .flatMap( category -> Stream.of( format( "%s (reserved)", category),  format( "%s (committed)", category) ) )
                .collect( joining( delimiter ) );

        try ( PrintWriter writer = new PrintWriter(
                Files.newBufferedWriter( path, StandardOpenOption.CREATE, StandardOpenOption.WRITE ), true /*auto flush*/  ) )
        {
            writer.println( headers );
            snapshots.stream()
                    .map( summary -> categories.stream()
                            .flatMap( category -> Stream.of(
                                    Long.toString( summary.getCategory( category ).getReserved() ),
                                    Long.toString( summary.getCategory( category ).getCommitted() ) ) )
                            .collect( joining( delimiter ) ) )
                    .forEach( writer::println );
        }
    }

}
