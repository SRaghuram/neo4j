/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.util;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class UnitConverter
{
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
}
