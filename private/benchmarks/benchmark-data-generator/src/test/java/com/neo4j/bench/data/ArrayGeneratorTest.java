/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.data.ArrayGenerator.byteArray;
import static com.neo4j.bench.data.ArrayGenerator.doubleArray;
import static com.neo4j.bench.data.ArrayGenerator.floatArray;
import static com.neo4j.bench.data.ArrayGenerator.intArray;
import static com.neo4j.bench.data.ArrayGenerator.longArray;
import static com.neo4j.bench.data.ArrayGenerator.shortArray;
import static com.neo4j.bench.data.ArrayGenerator.stringArray;
import static com.neo4j.bench.data.NumberGenerator.ascByte;
import static com.neo4j.bench.data.NumberGenerator.ascDouble;
import static com.neo4j.bench.data.NumberGenerator.ascFloat;
import static com.neo4j.bench.data.NumberGenerator.ascInt;
import static com.neo4j.bench.data.NumberGenerator.ascLong;
import static com.neo4j.bench.data.NumberGenerator.ascShort;
import static com.neo4j.bench.data.NumberGenerator.randByte;
import static com.neo4j.bench.data.NumberGenerator.randDouble;
import static com.neo4j.bench.data.NumberGenerator.randFloat;
import static com.neo4j.bench.data.NumberGenerator.randInt;
import static com.neo4j.bench.data.NumberGenerator.randLong;
import static com.neo4j.bench.data.NumberGenerator.randShort;
import static com.neo4j.bench.data.StringGenerator.SMALL_STRING_LENGTH;
import static com.neo4j.bench.data.StringGenerator.intString;
import static com.neo4j.bench.data.StringGenerator.randShortUtf8;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class ArrayGeneratorTest
{
    private static final Logger LOG = LoggerFactory.getLogger( ArrayGeneratorTest.class );
    // TODO add uniqueness test for ascending+sliding once it is implemented

    private static final int ITERATIONS = 100_000;
    private static final int LENGTH = 10;
    private static final long MAX_TOLERATED_RANDOM_GENERATION_DURATION = TimeUnit.SECONDS.toMillis( 10 );
    private static final long MAX_TOLERATED_ASCENDING_GENERATION_DURATION = TimeUnit.SECONDS.toMillis( 20 );

    @Test
    public void ascendingGeneratorsShouldDoNothingUnexpected() throws IOException
    {
        assertAscendingArrayGeneratorWorksAsAdvertised( intArray( ascInt( 0 ), LENGTH ), ITERATIONS );
        assertAscendingArrayGeneratorWorksAsAdvertised( longArray( ascLong( 0 ), LENGTH ), ITERATIONS );
        assertAscendingArrayGeneratorWorksAsAdvertised( floatArray( ascFloat( 0 ), LENGTH ), ITERATIONS );
        assertAscendingArrayGeneratorWorksAsAdvertised( doubleArray( ascDouble( 0 ), LENGTH ), ITERATIONS );
        assertAscendingArrayGeneratorWorksAsAdvertised(
                stringArray( intString( ascInt( 0 ), SMALL_STRING_LENGTH ), LENGTH ), ITERATIONS );
    }

    @Test
    public void randomGeneratorsShouldDoNothingUnexpected() throws IOException
    {
        assertRandomArrayGeneratorWorksAsAdvertised( shortArray( randShort( (short) 0, Short.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertRandomArrayGeneratorWorksAsAdvertised( intArray( randInt( 0, Integer.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertRandomArrayGeneratorWorksAsAdvertised( intArray( randInt( 0, Integer.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertRandomArrayGeneratorWorksAsAdvertised( longArray( randLong( 0, Long.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertRandomArrayGeneratorWorksAsAdvertised( floatArray( randFloat( 0, Float.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertRandomArrayGeneratorWorksAsAdvertised( doubleArray( randDouble( 0, Double.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertRandomArrayGeneratorWorksAsAdvertised( stringArray( randShortUtf8(), LENGTH ), ITERATIONS );
    }

    @Test
    public void ascendingArrayGeneratorsShouldBeDeterministic() throws IOException
    {
        assertGeneratorsAreDeterministic( byteArray( ascByte( (byte) 0 ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( shortArray( ascShort( (short) 0 ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( intArray( ascInt( 0 ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( longArray( ascLong( 0 ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( floatArray( ascFloat( 0 ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( doubleArray( ascDouble( 0 ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic(
                stringArray( intString( ascInt( 0 ), SMALL_STRING_LENGTH ), LENGTH ), ITERATIONS );
    }

    @Test
    public void randomArrayGeneratorsShouldBeDeterministic() throws IOException
    {
        assertGeneratorsAreDeterministic( byteArray( randByte( (byte) 0, Byte.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( shortArray( randShort( (short) 0, Short.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( intArray( randInt( 0, Integer.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( longArray( randLong( 0, Long.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( floatArray( randFloat( 0, Float.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( doubleArray( randDouble( 0, Double.MAX_VALUE ), LENGTH ), ITERATIONS );
        assertGeneratorsAreDeterministic( stringArray( randShortUtf8(), LENGTH ), ITERATIONS );
    }

    private void assertRandomArrayGeneratorWorksAsAdvertised( ValueGeneratorFactory arrayGenerator, int iterations )
    {
        long start = System.currentTimeMillis();
        int repeatedValueCount = 0;
        int measurements = 0;
        SplittableRandom rng = SplittableRandomProvider.newRandom( 42 );
        ValueGeneratorFun<?> fun = arrayGenerator.create();
        List previous = ValueGeneratorTestUtil.toList( fun.next( rng ) );
        for ( int i = 0; i < iterations; i++ )
        {
            List current = ValueGeneratorTestUtil.toList( fun.next( rng ) );
            if ( current.equals( previous ) )
            {
                repeatedValueCount++;
            }
            measurements++;

            // Assert the contents of each individual array is as advertised
            Object previousElement = null;
            for ( Object currentElement : current )
            {
                if ( null != previousElement && currentElement.equals( previousElement ) )
                {
                    repeatedValueCount++;
                }
                measurements++;
                previousElement = currentElement;
            }

            previous = current;
        }
        long duration = System.currentTimeMillis() - start;
        int toleratedRepetitions = (int) (0.001 * measurements);
        LOG.debug( format( "%s: Tolerated Repetitions = (%s/%s) , Observed = (%s,%s), Duration = %s (ms)",
                arrayGenerator, toleratedRepetitions, measurements, repeatedValueCount, measurements, duration ) );
        assertThat( "less than 0.01% value repetitions", repeatedValueCount, lessThan( toleratedRepetitions ) );
        assertThat( duration, lessThan( MAX_TOLERATED_RANDOM_GENERATION_DURATION ) );
    }

    private void assertAscendingArrayGeneratorWorksAsAdvertised( ValueGeneratorFactory arrayGenerator, int iterations )
    {
        long start = System.currentTimeMillis();
        SplittableRandom rng = SplittableRandomProvider.newRandom( 42 );
        ValueGeneratorFun<?> fun = arrayGenerator.create();
        List previous = ValueGeneratorTestUtil.toList( fun.next( rng ) );
        for ( int i = 0; i < iterations; i++ )
        {
            List current = ValueGeneratorTestUtil.toList( fun.next( rng ) );
            assertThat( format( "%s returned repeated value", arrayGenerator ), current, not( equalTo( previous ) ) );

            // Assert the contents of each individual array is as advertised
            Comparable previousElement = null;
            for ( Object currentElement : current )
            {
                if ( null != previousElement )
                {
                    assertThat( ((Comparable) currentElement).compareTo( previousElement ), equalTo( 1 ) );
                }
                previousElement = (Comparable) currentElement;
            }

            previous = current;
        }
        long duration = System.currentTimeMillis() - start;
        LOG.debug( format( "%s: Duration = %s (ms)", arrayGenerator, duration ) );
        assertThat( duration, lessThan( MAX_TOLERATED_ASCENDING_GENERATION_DURATION ) );
    }

    private void assertGeneratorsAreDeterministic( ValueGeneratorFactory arrayGenerator, int iterations )
    {
        SplittableRandom rng1 = SplittableRandomProvider.newRandom( 42 );
        SplittableRandom rng2 = SplittableRandomProvider.newRandom( 42 );
        ValueGeneratorFun<?> fun1 = arrayGenerator.create();
        ValueGeneratorFun<?> fun2 = arrayGenerator.create();

        long start = System.currentTimeMillis();
        for ( int i = 0; i < iterations; i++ )
        {
            Object value1 = fun1.next( rng1 );
            Object value2 = fun2.next( rng2 );
            assertThat( value1, equalTo( value2 ) );
        }
        long duration = System.currentTimeMillis() - start;
        LOG.debug( format( "Duration = %s (ms)", duration ) );
        assertThat( duration, lessThan( MAX_TOLERATED_ASCENDING_GENERATION_DURATION ) );
    }
}
