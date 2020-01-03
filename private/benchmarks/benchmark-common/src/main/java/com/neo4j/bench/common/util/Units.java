/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.neo4j.bench.common.model.Benchmark.Mode;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Units
{
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

    /*
     * Return unit that results in lowest value, given the provide mode
     */
    public static TimeUnit minValueUnit( TimeUnit a, TimeUnit b, Mode mode )
    {
        switch ( mode )
        {
        // 1000000 us/op == 1000 ms/op == 1 s/op
        case SINGLE_SHOT:
        case LATENCY:
            return TimeUnit.values()[Math.max( a.ordinal(), b.ordinal() )];
        // 1 op/us == 1000 op/ms == 1000000 op/s
        case THROUGHPUT:
            return TimeUnit.values()[Math.min( a.ordinal(), b.ordinal() )];
        default:
            throw new IllegalArgumentException( format( "Unsupported mode: %s", mode.name() ) );
        }
    }

    /*
     * Return unit that results in highest value, given the provide mode
     */
    public static TimeUnit maxValueUnit( TimeUnit a, TimeUnit b, Mode mode )
    {
        switch ( mode )
        {
        // 1000000 us/op == 1000 ms/op == 1 s/op
        case SINGLE_SHOT:
        case LATENCY:
            return TimeUnit.values()[Math.min( a.ordinal(), b.ordinal() )];
        // 1 op/us == 1000 op/ms == 1000000 op/s
        case THROUGHPUT:
            return TimeUnit.values()[Math.max( a.ordinal(), b.ordinal() )];
        default:
            throw new IllegalArgumentException( format( "Unsupported mode: %s", mode.name() ) );
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

    public static TimeUnit toSmallerValueUnit( TimeUnit unit, Mode mode )
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

    private static boolean hasSmallerValueUnit( TimeUnit unit, Mode mode )
    {
        switch ( mode )
        {
        // 1000000 us/op == 1000 ms/op == 1 s/op
        case LATENCY:
        case SINGLE_SHOT:
            return unit.ordinal() < SECONDS.ordinal();
        // 1 op/us == 1000 op/ms == 1000000 op/s
        case THROUGHPUT:
            return unit.ordinal() > NANOSECONDS.ordinal();
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

    private static boolean hasLargerValueUnit( TimeUnit unit, Mode mode )
    {
        switch ( mode )
        {
        // 1000000 us/op == 1000 ms/op == 1 s/op
        case LATENCY:
        case SINGLE_SHOT:
            return unit.ordinal() > NANOSECONDS.ordinal();
        // 1 op/us == 1000 op/ms == 1000000 op/s
        case THROUGHPUT:
            return unit.ordinal() < SECONDS.ordinal();
        default:
            throw new IllegalArgumentException( format( "Unsupported mode: %s", mode.name() ) );
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
        double factor = (from.ordinal() > to.ordinal())
                        ? to.convert( 1, from )
                        : 1D / from.convert( 1, to );
        return (mode.equals( Mode.THROUGHPUT )) ? 1D / factor : factor;
    }

    /**
     * Find unit where value is in the range [min,max]
     *
     * @param value value, at original unit
     * @param unit original unit
     * @param mode mode, throughput or latency
     * @return new unit, where value will be larger than zero
     */
    public static TimeUnit findSaneUnit( double value, TimeUnit unit, Mode mode, double min, double max )
    {
        while ( value < min && Units.hasLargerValueUnit( unit, mode ) )
        {
            value = value * 1000;
            unit = Units.toLargerValueUnit( unit, mode );
        }
        while ( value > max && Units.hasSmallerValueUnit( unit, mode ) )
        {
            value = value / 1000;
            unit = Units.toSmallerValueUnit( unit, mode );
        }
        return unit;
    }

    /**
     * Computes the speedup factor between old and new results.
     * Assumes both values are in the same unit.
     * <p>
     * E.g., if old=100 (tx/ms) & new=120 (tx/ms) improvement=1.2x
     */
    public static double improvement( double oldValue, double newValue, Mode mode )
    {
        switch ( mode )
        {
        case THROUGHPUT:
            return (oldValue < newValue)
                   ? newValue / oldValue
                   : -oldValue / newValue;
        case SINGLE_SHOT:
        case LATENCY:
            return (oldValue < newValue)
                   ? -newValue / oldValue
                   : oldValue / newValue;
        default:
            throw new IllegalArgumentException( "Unrecognized mode: " + mode );
        }
    }
}
