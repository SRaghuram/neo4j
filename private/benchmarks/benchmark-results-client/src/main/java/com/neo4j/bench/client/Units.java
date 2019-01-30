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

    // conversion factor is necessary because TimeUnit convert can only deal with long values
    public static double conversionFactor( TimeUnit from, TimeUnit to, Mode mode )
    {
        double factor = (UNITS.indexOf( from ) > UNITS.indexOf( to ))
                        ? to.convert( 1, from )
                        : 1D / from.convert( 1, to );
        return (mode.equals( Mode.THROUGHPUT )) ? 1D / factor : factor;
    }
}
