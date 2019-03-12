/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.model.Benchmark.Mode;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Units
{
    private static final List<TimeUnit> UNITS = newArrayList( NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS );

    public static String toAbbreviation( TimeUnit timeUnit, Mode mode )
    {
        return (mode.equals( Mode.THROUGHPUT ))
               ? "op/" + toAbbreviation( timeUnit )
               : toAbbreviation( timeUnit ) + "/op";
    }

    public static String toAbbreviation( TimeUnit timeUnit )
    {
        switch ( timeUnit )
        {
        case SECONDS:
            return "s";
        case MILLISECONDS:
            return "ms";
        case MICROSECONDS:
            return "us";
        case NANOSECONDS:
            return "ns";
        default:
            throw new RuntimeException( "Unsupported time unit: " + timeUnit );
        }
    }

    public static TimeUnit toTimeUnit( String timeUnit )
    {
        switch ( timeUnit )
        {
        case "s":
            return SECONDS;
        case "ms":
            return MILLISECONDS;
        case "us":
            return MICROSECONDS;
        case "ns":
            return NANOSECONDS;
        default:
            throw new RuntimeException( "Unsupported time unit: " + timeUnit );
        }
    }

    public static TimeUnit smallerValueUnit( TimeUnit unit, Mode mode )
    {
        switch ( mode )
        {
        // 1000000 us/op == 1000 ms/op == 1 s/op
        case LATENCY:
        case SINGLE_SHOT:
            return largerUnit( unit );
        // 1 op/us == 1000 op/ms == 1000000 op/s
        case THROUGHPUT:
            return smallerUnit( unit );
        default:
            throw new IllegalArgumentException( format( "Unable to convert %s %s to smaller unit", unit.name(), mode.name() ) );
        }
    }

    public static TimeUnit toLargerValueUnit( TimeUnit unit, Mode mode )
    {
        switch ( mode )
        {
        // 1000000 us/op == 1000 ms/op == 1 s/op
        case LATENCY:
        case SINGLE_SHOT:
            return smallerUnit( unit );
        // 1 op/us == 1000 op/ms == 1000000 op/s
        case THROUGHPUT:
            return largerUnit( unit );
        default:
            throw new IllegalArgumentException( format( "Unable to convert %s %s to smaller unit", unit.name(), mode.name() ) );
        }
    }

    private static TimeUnit smallerUnit( TimeUnit unit )
    {
        try
        {
            return TimeUnit.values()[unit.ordinal() - 1];
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( format( "Unable to convert %s to smaller unit", unit.name() ) );
        }
    }

    private static TimeUnit largerUnit( TimeUnit unit )
    {
        try
        {
            return TimeUnit.values()[unit.ordinal() + 1];
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( format( "Unable to convert %s to larger unit", unit.name() ) );
        }
    }

    public static double convertValueTo( double fromValue, TimeUnit fromUnit, TimeUnit toUnit, Mode mode )
    {
        return fromValue * Units.conversionFactor( fromUnit, toUnit, mode );
    }

    // conversion factor is necessary because TimeUnit convert can only deal with long values
    public static double conversionFactor( TimeUnit from, TimeUnit to, Mode mode )
    {
        double factor = (UNITS.indexOf( from ) > UNITS.indexOf( to ))
                        ? to.convert( 1, from )
                        : 1D / from.convert( 1, to );
        return (mode.equals( Mode.THROUGHPUT )) ? 1D / factor : factor;
    }
}
