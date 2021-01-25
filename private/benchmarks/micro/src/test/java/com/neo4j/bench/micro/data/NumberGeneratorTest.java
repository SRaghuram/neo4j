/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.micro.benchmarks.RNGState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.values.storable.Values;

import static com.neo4j.bench.micro.data.NumberGenerator.ascDouble;
import static com.neo4j.bench.micro.data.NumberGenerator.ascFloat;
import static com.neo4j.bench.micro.data.NumberGenerator.ascInt;
import static com.neo4j.bench.micro.data.NumberGenerator.ascLong;
import static com.neo4j.bench.micro.data.NumberGenerator.randDouble;
import static com.neo4j.bench.micro.data.NumberGenerator.randFloat;
import static com.neo4j.bench.micro.data.NumberGenerator.randInt;
import static com.neo4j.bench.micro.data.NumberGenerator.randLong;
import static com.neo4j.bench.micro.data.NumberGenerator.stridingDouble;
import static com.neo4j.bench.micro.data.NumberGenerator.stridingFloat;
import static com.neo4j.bench.micro.data.NumberGenerator.stridingInt;
import static com.neo4j.bench.micro.data.NumberGenerator.stridingLong;
import static com.neo4j.bench.micro.data.NumberGenerator.toDouble;
import static com.neo4j.bench.micro.data.NumberGenerator.toFloat;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.lang.String.format;

public class NumberGeneratorTest
{
    private static final Logger LOG = LoggerFactory.getLogger( NumberGeneratorTest.class );
    private static final int REPETITIONS = 1_000_000;
    private static final long MAX_TOLERATED_DURATION = TimeUnit.SECONDS.toMillis( 5 );

    @Test
    public void generatorsShouldDoNothingUnexpected() throws IOException
    {
        assertAscendingNumberGeneratorWorksAsAdvertised( ascInt( 0 ) );
        assertAscendingNumberGeneratorWorksAsAdvertised( ascLong( 0 ) );
        assertAscendingNumberGeneratorWorksAsAdvertised( ascFloat( 0 ) );
        assertAscendingNumberGeneratorWorksAsAdvertised( ascDouble( 0 ) );

        assertAscendingNumberGeneratorWorksAsAdvertised( toFloat( ascInt( 0 ) ) );
        assertAscendingNumberGeneratorWorksAsAdvertised( toDouble( ascLong( 0 ) ) );

        assertRandomNumberGeneratorWorksAsAdvertised( randInt( 0, Integer.MAX_VALUE ) );
        assertRandomNumberGeneratorWorksAsAdvertised( randLong( 0, Long.MAX_VALUE ) );
        assertRandomNumberGeneratorWorksAsAdvertised( randFloat( 0, Float.MAX_VALUE ) );
        assertRandomNumberGeneratorWorksAsAdvertised( randDouble( 0, Double.MAX_VALUE ) );

        assertRandomNumberGeneratorWorksAsAdvertised( toFloat( randInt( 0, Integer.MAX_VALUE ) ) );
        assertRandomNumberGeneratorWorksAsAdvertised( toDouble( randLong( 0, Long.MAX_VALUE ) ) );

        int stride = 5;
        int max = 1_000;
        int offset = 7;
        boolean sliding = true;
        assertRandomNumberGeneratorWorksAsAdvertised( stridingInt( stride, max, offset, sliding ) );
        assertRandomNumberGeneratorWorksAsAdvertised( stridingLong( stride, max, offset, sliding ) );
        assertRandomNumberGeneratorWorksAsAdvertised( stridingFloat( stride, max, offset, sliding ) );
        assertRandomNumberGeneratorWorksAsAdvertised( stridingDouble( stride, max, offset, sliding ) );

        assertRandomNumberGeneratorWorksAsAdvertised( toFloat( stridingInt( stride, max, offset, sliding ) ) );
        assertRandomNumberGeneratorWorksAsAdvertised( toDouble( stridingLong( stride, max, offset, sliding ) ) );

        assertGeneratorIsDeterministic( ascInt( 0 ) );
        assertGeneratorIsDeterministic( ascLong( 0 ) );
        assertGeneratorIsDeterministic( ascFloat( 0 ) );
        assertGeneratorIsDeterministic( ascDouble( 0 ) );

        assertGeneratorIsDeterministic( randInt( 0, Integer.MAX_VALUE ) );
        assertGeneratorIsDeterministic( randLong( 0, Long.MAX_VALUE ) );
        assertGeneratorIsDeterministic( randFloat( 0, Float.MAX_VALUE ) );
        assertGeneratorIsDeterministic( randDouble( 0, Double.MAX_VALUE ) );

        assertGeneratorIsDeterministic( toFloat( randInt( 0, Integer.MAX_VALUE ) ) );
        assertGeneratorIsDeterministic( toDouble( randLong( 0, Long.MAX_VALUE ) ) );

        assertGeneratorIsDeterministic( stridingInt( stride, max, offset, sliding ) );
        assertGeneratorIsDeterministic( stridingLong( stride, max, offset, sliding ) );
        assertGeneratorIsDeterministic( stridingFloat( stride, max, offset, sliding ) );
        assertGeneratorIsDeterministic( stridingDouble( stride, max, offset, sliding ) );

        assertGeneratorIsDeterministic( toFloat( stridingInt( stride, max, offset, sliding ) ) );
        assertGeneratorIsDeterministic( toFloat( stridingLong( stride, max, offset, sliding ) ) );

        sliding = false;
        assertRandomNumberGeneratorWorksAsAdvertised( stridingInt( stride, max, offset, sliding ) );
        assertRandomNumberGeneratorWorksAsAdvertised( stridingLong( stride, max, offset, sliding ) );
        assertRandomNumberGeneratorWorksAsAdvertised( stridingFloat( stride, max, offset, sliding ) );
        assertRandomNumberGeneratorWorksAsAdvertised( stridingDouble( stride, max, offset, sliding ) );

        assertGeneratorIsDeterministic( ascInt( 0 ) );
        assertGeneratorIsDeterministic( ascLong( 0 ) );
        assertGeneratorIsDeterministic( ascFloat( 0 ) );
        assertGeneratorIsDeterministic( ascDouble( 0 ) );

        assertGeneratorIsDeterministic( randInt( 0, Integer.MAX_VALUE ) );
        assertGeneratorIsDeterministic( randLong( 0, Long.MAX_VALUE ) );
        assertGeneratorIsDeterministic( randFloat( 0, Float.MAX_VALUE ) );
        assertGeneratorIsDeterministic( randDouble( 0, Double.MAX_VALUE ) );

        assertGeneratorIsDeterministic( stridingInt( stride, max, offset, sliding ) );
        assertGeneratorIsDeterministic( stridingLong( stride, max, offset, sliding ) );
        assertGeneratorIsDeterministic( stridingFloat( stride, max, offset, sliding ) );
        assertGeneratorIsDeterministic( stridingDouble( stride, max, offset, sliding ) );
    }

    private <NUMBER extends Number> void assertRandomNumberGeneratorWorksAsAdvertised(
            ValueGeneratorFactory<NUMBER> generatorFactory )
    {
        long start = System.currentTimeMillis();
        int repeatedValueCount = 0;
        SplittableRandom rng1 = RNGState.newRandom( 42 );
        SplittableRandom rng2 = RNGState.newRandom( 42 );
        ValueGeneratorFun<NUMBER> fun1 = generatorFactory.create();
        ValueGeneratorFun<NUMBER> fun2 = generatorFactory.create();
        NUMBER previous = fun1.next( rng1 );
        assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( previous ) ) );
        for ( int i = 0; i < REPETITIONS; i++ )
        {
            NUMBER current = fun1.next( rng1 );
            assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( current ) ) );
            if ( current.equals( previous ) )
            {
                repeatedValueCount++;
            }
            previous = current;
        }
        long duration = System.currentTimeMillis() - start;
        int toleratedRepetitions = (int) (0.001 * REPETITIONS);
        LOG.debug( format( "Tolerated Repetitions = %s , Observed Repetitions = %s, Duration = %s (ms)",
                                    toleratedRepetitions, repeatedValueCount, duration ) );
        assertThat( "less than 0.01% value repetitions", repeatedValueCount, lessThan( toleratedRepetitions ) );
        assertThat( duration, lessThan( MAX_TOLERATED_DURATION ) );
    }

    private <NUMBER extends Number> void assertAscendingNumberGeneratorWorksAsAdvertised(
            ValueGeneratorFactory<NUMBER> generatorFactory )
    {
        long start = System.currentTimeMillis();
        SplittableRandom rng1 = RNGState.newRandom( 42 );
        SplittableRandom rng2 = RNGState.newRandom( 42 );
        ValueGeneratorFun<NUMBER> fun1 = generatorFactory.create();
        ValueGeneratorFun<NUMBER> fun2 = generatorFactory.create();
        NUMBER previous = fun1.next( rng1 );
        assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( previous ) ) );
        for ( int i = 0; i < REPETITIONS; i++ )
        {
            NUMBER current = fun1.next( rng1 );
            assertThat( current.doubleValue(), greaterThan( previous.doubleValue() ) );
            assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( current ) ) );
            previous = current;
        }
        long duration = System.currentTimeMillis() - start;
        LOG.debug( format( "Duration = %s (ms)", duration ) );
        assertThat( duration, lessThan( MAX_TOLERATED_DURATION ) );
    }

    private <NUMBER extends Number> void assertGeneratorIsDeterministic(
            ValueGeneratorFactory<NUMBER> generatorFactory )
    {
        SplittableRandom rng1 = RNGState.newRandom( 42 );
        SplittableRandom rng2 = RNGState.newRandom( 42 );
        ValueGeneratorFun<NUMBER> fun1 = generatorFactory.create();
        ValueGeneratorFun<NUMBER> fun2 = generatorFactory.create();

        long start = System.currentTimeMillis();
        for ( int i = 0; i < REPETITIONS; i++ )
        {
            NUMBER value1 = fun1.next( rng1 );
            NUMBER value2 = fun2.next( rng2 );
            assertThat( value1, equalTo( value2 ) );
        }
        long duration = System.currentTimeMillis() - start;
        LOG.debug( format( "Duration = %s (ms)", duration ) );
        assertThat( duration, lessThan( MAX_TOLERATED_DURATION ) );
    }

    @Test
    public void shouldUniformlyCoverAllIdsInRangeWithStridingSliding()
    {
        SplittableRandom rng = RNGState.newRandom( 42 );
        int sampleSize = 1_000_000;
        int inputScenarios = 10;
        for ( int i = 0; i < inputScenarios; i++ )
        {
            int max = 10 + rng.nextInt( 1_000_000 - 10 );
            int stride = 1 + rng.nextInt( max - 1 );
            int offset = 0;
            boolean sliding = true;
            LOG.debug( format(
                    "Max = %s, Stride = %s, Offset = %s, Sliding = %s",
                    max, stride, offset, sliding ) );
            ValueGeneratorFun<Long> values = stridingLong( stride, max, offset, sliding ).create();
            Map<Long,Integer> idCounts = calculateIdCounts( values, sampleSize, rng );
            long uniqueIds = idCounts.keySet().stream().count();
            int minIdCount = idCounts.values().stream().mapToInt( value -> value ).min().getAsInt();
            int maxIdCount = idCounts.values().stream().mapToInt( value -> value ).max().getAsInt();
            long actualMax = idCounts.keySet().stream().mapToLong( Long::longValue ).max().getAsLong();
            LOG.debug( format(
                    "Unique IDs = %s, Min ID Count = %s, Max ID Count = %s, Actual Max = %s",
                    uniqueIds, minIdCount, maxIdCount, actualMax ) );
            assertThat( (int) uniqueIds, equalTo( max ) );
            assertThat( maxIdCount - minIdCount, anyOf( equalTo( 0 ), equalTo( 1 ) ) );
        }
    }

    private Map<Long,Integer> calculateIdCounts( ValueGeneratorFun<Long> values, int sampleSize, SplittableRandom rng )
    {
        Map<Long,Integer> idCounts = new HashMap<>();
        for ( int i = 0; i < sampleSize; i++ )
        {
            long id = values.next( rng );
            int count = idCounts.computeIfAbsent( id, key -> 0 );
            idCounts.put( id, count + 1 );
        }
        return idCounts;
    }

    @Test
    public void shouldThrowExceptionWhenStrideLowerThan1()
    {
        int max = 10;
        int stride = 0;
        int offset = 1;
        boolean sliding = false;
        assertThrows( Exception.class, () ->
        {
            stridingLong( stride, max, offset, sliding ).create();
        });
    }

    @Test
    public void shouldThrowExceptionWhenStrideGreaterOrEqualToMax()
    {
        int max = 10;
        int stride = 10;
        int offset = 1;
        boolean sliding = false;
        assertThrows( Exception.class, () ->
        {
            stridingLong( stride, max, offset, sliding ).create();
        });
    }

    @Test
    public void shouldThrowExceptionWhenOffsetLowerThan0()
    {
        int max = 10;
        int stride = 1;
        int offset = -1;
        boolean sliding = false;
        assertThrows( Exception.class, () ->
        {
            stridingLong( stride, max, offset, sliding ).create();
        });
    }

    @Test
    public void shouldThrowExceptionWhenOffsetGreaterOrEqualToMax()
    {
        int max = 10;
        int stride = 1;
        int offset = 10;
        boolean sliding = false;
        assertThrows( Exception.class, () ->
        {
            stridingLong( stride, max, offset, sliding ).create();
        });
    }

    @Test
    public void shouldCreateExpectedStridingSequenceWithoutSlidingAndOffsetBelowStride()
    {
        int max = 10;
        int stride = 2;
        int offset = 1;
        boolean sliding = false;
        SplittableRandom rng = RNGState.newRandom( 42 );
        ValueGeneratorFun<Long> values = stridingLong( stride, max, offset, sliding ).create();

        assertThat( values.next( rng ), equalTo( 1L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 3L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 5L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 7L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 9L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 1L ) );         // <-- wraps here
        assertThat( values.wrapped(), equalTo( true ) );
        assertThat( values.next( rng ), equalTo( 3L ) );
        assertThat( values.wrapped(), equalTo( false ) );
    }

    @Test
    public void shouldCreateExpectedStridingSequenceWithoutSlidingAndOffsetAboveStride()
    {
        int max = 10;
        int stride = 2;
        int offset = 6;
        boolean sliding = false;
        SplittableRandom rng = RNGState.newRandom( 42 );
        ValueGeneratorFun<Long> values = stridingLong( stride, max, offset, sliding ).create();

        assertThat( values.next( rng ), equalTo( 6L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 8L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 0L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 2L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 4L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 6L ) );         // <-- wraps here
        assertThat( values.wrapped(), equalTo( true ) );
        assertThat( values.next( rng ), equalTo( 8L ) );
        assertThat( values.wrapped(), equalTo( false ) );
    }

    @Test
    public void shouldCreateExpectedStridingSequenceWithSlidingAndOffsetBelowStride()
    {
        int max = 10;
        int stride = 2;
        int offset = 1;
        boolean sliding = true;
        SplittableRandom rng = RNGState.newRandom( 42 );
        ValueGeneratorFun<Long> values = stridingLong( stride, max, offset, sliding ).create();

        assertThat( values.next( rng ), equalTo( 1L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 3L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 5L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 7L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 9L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 0L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 2L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 4L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 6L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 8L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 1L ) );         // <-- wraps here
        assertThat( values.wrapped(), equalTo( true ) );
        assertThat( values.next( rng ), equalTo( 3L ) );
        assertThat( values.wrapped(), equalTo( false ) );
    }

    @Test
    public void shouldCreateExpectedStridingSequenceWithSlidingOffsetAboveStride()
    {
        int max = 10;
        int stride = 3;
        int offset = 5;
        boolean sliding = true;
        SplittableRandom rng = RNGState.newRandom( 42 );
        ValueGeneratorFun<Long> values = stridingLong( stride, max, offset, sliding ).create();

        assertThat( values.next( rng ), equalTo( 5L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 8L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 0L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 3L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 6L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 9L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 1L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 4L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 7L ) );
        assertThat( values.wrapped(), equalTo( false ) );

        assertThat( values.next( rng ), equalTo( 2L ) );
        assertThat( values.wrapped(), equalTo( false ) );
        assertThat( values.next( rng ), equalTo( 5L ) );         // <-- wraps here
        assertThat( values.wrapped(), equalTo( true ) );
        assertThat( values.next( rng ), equalTo( 8L ) );
        assertThat( values.wrapped(), equalTo( false ) );
    }
}
