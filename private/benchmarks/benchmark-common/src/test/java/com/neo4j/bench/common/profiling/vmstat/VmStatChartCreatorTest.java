/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.vmstat;

import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.common.profiling.metrics.Axis;
import com.neo4j.bench.common.profiling.metrics.Chart;
import com.neo4j.bench.common.profiling.metrics.Layout;
import com.neo4j.bench.common.profiling.metrics.Point;
import com.neo4j.bench.common.profiling.metrics.Series;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class VmStatChartCreatorTest
{
    @Test
    public void shouldCreateChartFromRawPoints()
    {
        Instant instant = LocalDateTime.of( 2020, 12, 3, 13, 28, 37 ).toInstant( OffsetDateTime.now().getOffset() );
        Map<String,List<Point>> data = new HashMap<String,List<Point>>()
        {{
            put( "r", singletonList( new Point( 1, instant ) ) );
            put( "b", singletonList( new Point( 2, instant ) ) );
            put( "swpd", singletonList( new Point( 3, instant ) ) );
            put( "free", singletonList( new Point( 4, instant ) ) );
            put( "buff", singletonList( new Point( 5, instant ) ) );
            put( "cache", singletonList( new Point( 6, instant ) ) );
            put( "si", singletonList( new Point( 7, instant ) ) );
            put( "so", singletonList( new Point( 8, instant ) ) );
            put( "bi", singletonList( new Point( 9, instant ) ) );
            put( "bo", singletonList( new Point( 10, instant ) ) );
            put( "in", singletonList( new Point( 11, instant ) ) );
            put( "cs", singletonList( new Point( 12, instant ) ) );
            put( "us", singletonList( new Point( 13, instant ) ) );
            put( "sy", singletonList( new Point( 14, instant ) ) );
            put( "id", singletonList( new Point( 15, instant ) ) );
            put( "wa", singletonList( new Point( 16, instant ) ) );
            put( "st", singletonList( new Point( 17, instant ) ) );
        }};

        List<Chart> result = VmStatChartCreator.createCharts( data );

        List<Long> timestamps = singletonList( 1606998517000L );
        List<Chart> expected = Arrays.asList(
                new Chart( new Layout( "Procs", new Axis( "" ) ), Arrays.asList(
                        new Series( "r: The number of runnable processes (running or waiting for run time)", timestamps, singletonList( 1.0 ) ),
                        new Series( "b: The number of processes in uninterruptible sleep", timestamps, singletonList( 2.0 ) ) ) ),
                new Chart( new Layout( "Memory", new Axis( "MiB" ) ), Arrays.asList(
                        new Series( "swpd: the amount of virtual memory used", timestamps, singletonList( 3.0 ) ),
                        new Series( "free: the amount of idle memory", timestamps, singletonList( 4.0 ) ),
                        new Series( "buff: the amount of memory used as buffers", timestamps, singletonList( 5.0 ) ),
                        new Series( "cache: the amount of memory used as cache", timestamps, singletonList( 6.0 ) ) ) ),
                new Chart( new Layout( "Swap", new Axis( "MiB/s" ) ), Arrays.asList(
                        new Series( "si: Amount of memory swapped in from disk", timestamps, singletonList( 7.0 ) ),
                        new Series( "so: Amount of memory swapped to disk", timestamps, singletonList( 8.0 ) ) ) ),
                new Chart( new Layout( "IO", new Axis( "blocks/s" ) ), Arrays.asList(
                        new Series( "bi: Blocks received from a block device", timestamps, singletonList( 9.0 ) ),
                        new Series( "bo: Blocks sent to a block device", timestamps, singletonList( 10.0 ) ) ) ),
                new Chart( new Layout( "System", new Axis( "ops/s" ) ), Arrays.asList(
                        new Series( "in: The number of interrupts per second, including the clock", timestamps, singletonList( 11.0 ) ),
                        new Series( "cs: The number of context switches per second", timestamps, singletonList( 12.0 ) ) ) ),
                new Chart( new Layout( "CPU", new Axis( "% of total CPU time" ) ), Arrays.asList(
                        new Series( "us: Time spent running non-kernel code. (user time, including nice time)", timestamps, singletonList( 13.0 ) ),
                        new Series( "sy: Time spent running kernel code. (system time)", timestamps, singletonList( 14.0 ) ),
                        new Series( "id: Time spent idle", timestamps, singletonList( 15.0 ) ),
                        new Series( "wa: Time spent waiting for IO", timestamps, singletonList( 16.0 ) ),
                        new Series( "st: Time stolen from a virtual machine", timestamps, singletonList( 17.0 ) ) ) ) );

        assertThat( result, equalTo( expected ) );
    }

    @Test
    public void shouldIgnoreUnknownSeries()
    {
        Map<String,List<Point>> data = ImmutableMap.of( "unknown", singletonList( new Point( 1, Instant.now() ) ) );

        List<Chart> result = VmStatChartCreator.createCharts( data );

        assertThat( result, equalTo( Collections.<Chart>emptyList() ) );
    }
}
