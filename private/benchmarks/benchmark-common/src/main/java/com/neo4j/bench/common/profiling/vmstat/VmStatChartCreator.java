/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.vmstat;

import com.neo4j.bench.common.profiling.metrics.Axis;
import com.neo4j.bench.common.profiling.metrics.Chart;
import com.neo4j.bench.common.profiling.metrics.Layout;
import com.neo4j.bench.common.profiling.metrics.Point;
import com.neo4j.bench.common.profiling.metrics.Series;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class VmStatChartCreator
{
    // Note LinkedHashMap because we want to preserve order
    private static final Map<String,String> PROCS = new LinkedHashMap<String,String>()
    {{
        put( "r", "The number of runnable processes (running or waiting for run time)" );
        put( "b", "The number of processes in uninterruptible sleep" );
    }};
    private static final Map<String,String> SWAP = new LinkedHashMap<String,String>()
    {{
        put( "si", "Amount of memory swapped in from disk" );
        put( "so", "Amount of memory swapped to disk" );
    }};
    private static final Map<String,String> IO = new LinkedHashMap<String,String>()
    {{
        put( "bi", "Blocks received from a block device" );
        put( "bo", "Blocks sent to a block device" );
    }};
    private static final Map<String,String> SYSTEM = new LinkedHashMap<String,String>()
    {{
        put( "in", "The number of interrupts per second, including the clock" );
        put( "cs", "The number of context switches per second" );
    }};
    private static final Map<String,String> CPU = new LinkedHashMap<String,String>()
    {{
        put( "us", "Time spent running non-kernel code. (user time, including nice time)" );
        put( "sy", "Time spent running kernel code. (system time)" );
        put( "id", "Time spent idle" );
        put( "wa", "Time spent waiting for IO" );
        put( "st", "Time stolen from a virtual machine" );
    }};
    private static final Map<String,String> MEMORY = new LinkedHashMap<String,String>()
    {{
        put( "swpd", "the amount of virtual memory used" );
        put( "free", "the amount of idle memory" );
        put( "buff", "the amount of memory used as buffers" );
        put( "cache", "the amount of memory used as cache" );
    }};
    private static final Map<Layout,Map<String,String>> DESCRIPTIONS = new LinkedHashMap<Layout,Map<String,String>>()
    {{
        put( new Layout( "Procs", new Axis( "" ) ), PROCS );
        put( new Layout( "Memory", new Axis( "MiB" ) ), MEMORY );
        put( new Layout( "Swap", new Axis( "MiB/s" ) ), SWAP );
        put( new Layout( "IO", new Axis( "blocks/s" ) ), IO );
        put( new Layout( "System", new Axis( "ops/s" ) ), SYSTEM );
        put( new Layout( "CPU", new Axis( "% of total CPU time" ) ), CPU );
    }};

    static List<Chart> createCharts( Map<String,List<Point>> nameToPoints )
    {
        return DESCRIPTIONS.entrySet()
                           .stream()
                           .map( entry -> new Chart( entry.getKey(), createSeries( entry.getValue(), nameToPoints ) ) )
                           .filter( Chart::isDataPresent )
                           .collect( Collectors.toList() );
    }

    private static List<Series> createSeries( Map<String,String> nameToDescription, Map<String,List<Point>> nameToPoints )
    {
        return nameToDescription.entrySet()
                                .stream()
                                .map( entry -> createSingleSeries( entry.getKey(), entry.getValue(), nameToPoints ) )
                                .filter( Optional::isPresent )
                                .map( Optional::get )
                                .collect( Collectors.toList() );
    }

    private static Optional<Series> createSingleSeries( String name, String description, Map<String,List<Point>> nameToPoints )
    {
        if ( nameToPoints.containsKey( name ) )
        {
            return Optional.of( Series.toPlotlyChart( name + ": " + description, nameToPoints.get( name ) ) );
        }
        else
        {
            return Optional.empty();
        }
    }
}
