/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.nmt;

import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSummary;
import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSummaryParser;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NativeMemoryTrackingSummaryParserTest
{
    @Test
    public void parseNativeMemoryTrackingSummary() throws Exception
    {
        NativeMemoryTrackingSummaryParser parser = new NativeMemoryTrackingSummaryParser();
        NativeMemoryTrackingSummary summary = parser.parse( Paths.get(
                "src/test/resources/NativeMemoryTrackingSummaryReportTest/test.nmt.summary" ) );

        assertEquals( 4194304, summary.getCategory( "Java Heap" ).getReserved() );
        assertEquals( 4194304, summary.getCategory( "Java Heap" ).getCommitted() );

        assertEquals( 1112175, summary.getCategory( "Class" ).getReserved() );
        assertEquals( 72391, summary.getCategory( "Class" ).getCommitted() );

        assertEquals( 36306, summary.getCategory( "Thread" ).getReserved() );
        assertEquals( 36306, summary.getCategory( "Thread" ).getCommitted() );

        assertEquals( 253990, summary.getCategory( "Code" ).getReserved() );
        assertEquals( 27790, summary.getCategory( "Code" ).getCommitted() );

        assertEquals( 163629, summary.getCategory( "GC" ).getReserved() );
        assertEquals( 163629, summary.getCategory( "GC" ).getCommitted() );

        assertEquals( 191, summary.getCategory( "Compiler" ).getReserved() );
        assertEquals( 191, summary.getCategory( "Compiler" ).getCommitted() );

        assertEquals( 628867, summary.getCategory( "Internal" ).getReserved() );
        assertEquals( 628867, summary.getCategory( "Internal" ).getCommitted() );

        assertEquals( 17798, summary.getCategory( "Symbol" ).getReserved() );
        assertEquals( 17798, summary.getCategory( "Symbol" ).getCommitted() );

        assertEquals( 2819, summary.getCategory( "Native Memory Tracking" ).getReserved() );
        assertEquals( 2819, summary.getCategory( "Native Memory Tracking" ).getCommitted() );

        assertEquals( 936, summary.getCategory( "Arena Chunk" ).getReserved() );
        assertEquals( 936, summary.getCategory( "Arena Chunk" ).getCommitted() );
    }
}
