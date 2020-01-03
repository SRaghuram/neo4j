/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.neo4j.bench.common.model.Benchmark.Mode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.common.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.common.model.Benchmark.Mode.SINGLE_SHOT;
import static com.neo4j.bench.common.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.common.util.Units.conversionFactor;
import static com.neo4j.bench.common.util.Units.convertValueTo;
import static com.neo4j.bench.common.util.Units.findSaneUnit;
import static com.neo4j.bench.common.util.Units.toAbbreviation;
import static com.neo4j.bench.common.util.Units.toLargerValueUnit;
import static com.neo4j.bench.common.util.Units.toSmallerValueUnit;
import static com.neo4j.bench.common.util.Units.toTimeUnit;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnitsTest
{
    @Test
    public void shouldReturnUnitForMinMaxValue()
    {
        // throughput
        assertThat( Units.minValueUnit( SECONDS, MILLISECONDS, THROUGHPUT ), equalTo( MILLISECONDS ) );
        assertThat( Units.maxValueUnit( SECONDS, MILLISECONDS, THROUGHPUT ), equalTo( SECONDS ) );
        // latency
        assertThat( Units.minValueUnit( SECONDS, MILLISECONDS, LATENCY ), equalTo( SECONDS ) );
        assertThat( Units.maxValueUnit( SECONDS, MILLISECONDS, LATENCY ), equalTo( MILLISECONDS ) );
    }

    @Test
    public void shouldFindSaneUnit()
    {
        // Throughput low value
        double originalValue = 0.01;
        TimeUnit originalUnit = MILLISECONDS;
        Mode mode = THROUGHPUT;
        TimeUnit expectedUnit = SECONDS;

        TimeUnit saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertThat( convertValueTo( originalValue, originalUnit, saneUnit, mode ), equalTo( 10.0 ) );

        // Throughput high value
        originalValue = 1001;
        originalUnit = MILLISECONDS;
        mode = THROUGHPUT;
        expectedUnit = MICROSECONDS;

        saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertWithinPercent( convertValueTo( originalValue, originalUnit, saneUnit, mode ), 1.001, 0.001 );

        // Latency low value
        originalValue = 0.01;
        originalUnit = MILLISECONDS;
        mode = LATENCY;
        expectedUnit = MICROSECONDS;

        saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertThat( convertValueTo( originalValue, originalUnit, saneUnit, mode ), equalTo( 10.0 ) );

        // Latency high value
        originalValue = 1001;
        originalUnit = MILLISECONDS;
        mode = LATENCY;
        expectedUnit = SECONDS;

        saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertWithinPercent( convertValueTo( originalValue, originalUnit, saneUnit, mode ), 1.001, 0.001 );
    }

    @Test
    public void shouldFindSaneUnitAtLimit()
    {
        // Throughput low value
        double originalValue = 0.01;
        TimeUnit originalUnit = SECONDS;
        Mode mode = THROUGHPUT;
        TimeUnit expectedUnit = SECONDS;

        TimeUnit saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertThat( convertValueTo( originalValue, originalUnit, saneUnit, mode ), equalTo( 0.01 ) );

        // Throughput high value
        originalValue = 1001;
        originalUnit = NANOSECONDS;
        mode = THROUGHPUT;
        expectedUnit = NANOSECONDS;

        saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertWithinPercent( convertValueTo( originalValue, originalUnit, saneUnit, mode ), 1001, 0.001 );

        // Latency low value
        originalValue = 0.01;
        originalUnit = NANOSECONDS;
        mode = LATENCY;
        expectedUnit = NANOSECONDS;

        saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertThat( convertValueTo( originalValue, originalUnit, saneUnit, mode ), equalTo( 0.01 ) );

        // Latency high value
        originalValue = 1001;
        originalUnit = SECONDS;
        mode = LATENCY;
        expectedUnit = SECONDS;

        saneUnit = findSaneUnit( originalValue, originalUnit, mode, 1, 1000 );
        assertThat( saneUnit, equalTo( expectedUnit ) );
        assertWithinPercent( convertValueTo( originalValue, originalUnit, saneUnit, mode ), 1001, 0.001 );
    }

    @Test
    public void shouldConvertToSmallerOrLargerValueUnit()
    {
        assertThat( toSmallerValueUnit( MILLISECONDS, LATENCY ), equalTo( SECONDS ) );
        assertThat( toSmallerValueUnit( MILLISECONDS, SINGLE_SHOT ), equalTo( SECONDS ) );
        assertThat( toSmallerValueUnit( MILLISECONDS, THROUGHPUT ), equalTo( MICROSECONDS ) );

        assertThat( toLargerValueUnit( MILLISECONDS, LATENCY ), equalTo( MICROSECONDS ) );
        assertThat( toLargerValueUnit( MILLISECONDS, SINGLE_SHOT ), equalTo( MICROSECONDS ) );
        assertThat( toLargerValueUnit( MILLISECONDS, THROUGHPUT ), equalTo( SECONDS ) );
    }

    @Test
    public void shouldAbbreviate()
    {
        assertThat( toAbbreviation( SECONDS ), equalTo( "s" ) );
        assertThat( toAbbreviation( MILLISECONDS ), equalTo( "ms" ) );
        assertThat( toAbbreviation( MICROSECONDS ), equalTo( "us" ) );
        assertThat( toAbbreviation( NANOSECONDS ), equalTo( "ns" ) );

        assertThat( toAbbreviation( SECONDS, THROUGHPUT ), equalTo( "op/s" ) );
        assertThat( toAbbreviation( MILLISECONDS, THROUGHPUT ), equalTo( "op/ms" ) );
        assertThat( toAbbreviation( MICROSECONDS, THROUGHPUT ), equalTo( "op/us" ) );
        assertThat( toAbbreviation( NANOSECONDS, THROUGHPUT ), equalTo( "op/ns" ) );

        assertThat( toAbbreviation( SECONDS, LATENCY ), equalTo( "s/op" ) );
        assertThat( toAbbreviation( MILLISECONDS, LATENCY ), equalTo( "ms/op" ) );
        assertThat( toAbbreviation( MICROSECONDS, LATENCY ), equalTo( "us/op" ) );
        assertThat( toAbbreviation( NANOSECONDS, LATENCY ), equalTo( "ns/op" ) );
    }

    @Test
    public void shouldConvertAbbreviationToTimeUnit()
    {
        assertThat( toTimeUnit( "s" ), equalTo( SECONDS ) );
        assertThat( toTimeUnit( "ms" ), equalTo( MILLISECONDS ) );
        assertThat( toTimeUnit( "us" ), equalTo( MICROSECONDS ) );
        assertThat( toTimeUnit( "ns" ), equalTo( NANOSECONDS ) );
    }

    @Test
    public void shouldComputeConversionFactor()
    {
        assertWithinPercent( conversionFactor( SECONDS, MILLISECONDS, LATENCY ), 1000D, 0.001 );
        assertWithinPercent( conversionFactor( SECONDS, MICROSECONDS, LATENCY ), 1000_000D, 0.001 );
        assertWithinPercent( conversionFactor( SECONDS, NANOSECONDS, LATENCY ), 1000_000_000D, 0.001 );

        assertWithinPercent( conversionFactor( MILLISECONDS, SECONDS, LATENCY ), 1 / 1000D, 0.001 );
        assertWithinPercent( conversionFactor( MICROSECONDS, SECONDS, LATENCY ), 1 / 1000_000D, 0.001 );
        assertWithinPercent( conversionFactor( NANOSECONDS, SECONDS, LATENCY ), 1 / 1000_000_000D, 0.001 );

        assertWithinPercent( conversionFactor( SECONDS, MILLISECONDS, THROUGHPUT ), 1 / 1000D, 0.001 );
        assertWithinPercent( conversionFactor( SECONDS, MICROSECONDS, THROUGHPUT ), 1 / 1000_000D, 0.001 );
        assertWithinPercent( conversionFactor( SECONDS, NANOSECONDS, THROUGHPUT ), 1 / 1000_000_000D, 0.001 );

        assertWithinPercent( conversionFactor( MILLISECONDS, SECONDS, THROUGHPUT ), 1000D, 0.001 );
        assertWithinPercent( conversionFactor( MICROSECONDS, SECONDS, THROUGHPUT ), 1000_000D, 0.001 );
        assertWithinPercent( conversionFactor( NANOSECONDS, SECONDS, THROUGHPUT ), 1000_000_000D, 0.001 );
    }

    private static void assertWithinPercent( double val1, double val2, double percent )
    {
        assertTrue( val2 > val1 - (val1 * percent) );
        assertTrue( val2 < val1 + (val1 * percent) );
    }
}
