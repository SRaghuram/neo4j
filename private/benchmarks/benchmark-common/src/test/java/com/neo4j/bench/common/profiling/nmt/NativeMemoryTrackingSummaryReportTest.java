/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
public class NativeMemoryTrackingSummaryReportTest
{

    @Inject
    public TestDirectory temporaryFolder;

    @Test
    public void createSummaryReport() throws IOException
    {
        // given
        Path forkDirectory = Paths.get( "src/test/resources/nmt" );

        // when
        NativeMemoryTrackingSummaryReport report =
                NativeMemoryTrackingSummaryReport.create( forkDirectory );

        // then
        assertArrayEquals(
                new long[] {4194304, 4194304, 4194304, 4194304, 4194304, 4194304},
                report.getReservedKBInCategory( "Java Heap" ) );
        assertArrayEquals(
                new long[] {4194304, 4194304, 4194304, 4194304, 4194304, 4194304},
                report.getCommittedKBInCategory( "Java Heap" ) );

        assertArrayEquals(
                new long[] {1112175, 1112170, 1112170, 1112170, 1112170, 1112171},
                report.getReservedKBInCategory( "Class" ) );
        assertArrayEquals(
                new long[] {72391, 72642, 72642, 72642, 72642, 72643},
                report.getCommittedKBInCategory( "Class" ) );

        assertArrayEquals(
                new long[] {36306, 36539, 36539, 36539, 36539, 36539},
                report.getReservedKBInCategory( "Thread" ) );
        assertArrayEquals(
                new long[] {36306, 36539, 36539, 36539, 36539, 36539},
                report.getCommittedKBInCategory( "Thread" ) );

        assertArrayEquals(
                new long[] {253990, 253533, 253536, 253522, 253523, 253374},
                report.getReservedKBInCategory( "Code" ) );
        assertArrayEquals(
                new long[] {27790, 27333, 27336, 27322, 27323, 27174},
                report.getCommittedKBInCategory( "Code" ) );

        // when
        File tmpfile = temporaryFolder.createFile( "nmt_summary.csv" ).toFile();
        report.toCSV( tmpfile.toPath() );

        // then
        assertEquals( 7, Files.readAllLines( tmpfile.toPath() ).stream().count() );
        Map<String, long[]> rows = fromCSV( tmpfile );
        assertArrayEquals(
                new long[] {4194304, 4194304, 4194304, 4194304, 4194304, 4194304},
                rows.get( "Java Heap (reserved)" ) );
        assertArrayEquals(
                new long[] {4194304, 4194304, 4194304, 4194304, 4194304, 4194304},
                rows.get( "Java Heap (committed)" ) );

        assertArrayEquals(
                new long[] {1112175, 1112170, 1112170, 1112170, 1112170, 1112171},
                rows.get( "Class (reserved)" ) );
        assertArrayEquals(
                new long[] {72391, 72642, 72642, 72642, 72642, 72643},
                rows.get( "Class (committed)" ) );

        assertArrayEquals(
                new long[] {36306, 36539, 36539, 36539, 36539, 36539},
                rows.get( "Thread (reserved)" ) );
        assertArrayEquals(
                new long[] {36306, 36539, 36539, 36539, 36539, 36539},
                rows.get( "Thread (committed)" ) );

        assertArrayEquals(
                new long[] {253990, 253533, 253536, 253522, 253523, 253374},
                rows.get( "Code (reserved)" ) );
        assertArrayEquals(
                new long[] {27790, 27333, 27336, 27322, 27323, 27174},
                rows.get( "Code (committed)" ) );

    }

    private Map<String,long[]> fromCSV( File csvFile ) throws IOException
    {
        List<String> header = Files.lines( csvFile.toPath() )
                .findFirst()
                .map( row -> row.split( "," ) )
                .map( Arrays::asList )
                .get();

        try ( Stream<String> rows = Files.lines( csvFile.toPath() ).skip( 1 ) )
        {
            return rows.map( row -> row.split( "," ) )
                .map( Arrays::asList )
                .map( row ->
                {
                    Map<String,Long> mappedRow = new HashMap<>();
                    for ( int i = 0; i < row.size(); i++ )
                    {
                        mappedRow.put( header.get( i ), Long.parseLong( row.get( i ) ) );
                    }
                    return mappedRow;
                } )
                .flatMap( row -> row.entrySet().stream() )
                .collect(
                        groupingBy( Map.Entry::getKey,
                        collectingAndThen(
                            mapping( Map.Entry::getValue, toList() ),
                            column -> column.stream().mapToLong( Long::valueOf ).toArray() ) ) );
        }
    }
}
