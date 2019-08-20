/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

import com.neo4j.bench.common.results.ForkDirectory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NativeMemoryTrackingSummaryReportTest
{

    @Test
    void createSummaryReport() throws IOException
    {
        // given
        ForkDirectory forkDirectory = ForkDirectory.openAt( Paths.get( "src/test/resources/NativeMemoryTrackingSummaryReportTest" ) );

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
        Path path = Files.createTempFile( "test", "csv" );
        report.toCSV( path );

        // then
        assertEquals( 7, Files.readAllLines( path ).stream().count() );
    }

}
