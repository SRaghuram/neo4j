/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.micro.benchmarks.RNGState;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.values.storable.Values;

import static com.neo4j.bench.micro.data.NumberGenerator.ascInt;
import static com.neo4j.bench.micro.data.StringGenerator.BIG_STRING_LENGTH;
import static com.neo4j.bench.micro.data.StringGenerator.SMALL_STRING_LENGTH;
import static com.neo4j.bench.micro.data.StringGenerator.intString;
import static com.neo4j.bench.micro.data.StringGenerator.randShortAlphaNumerical;
import static com.neo4j.bench.micro.data.StringGenerator.randShortAlphaSymbolical;
import static com.neo4j.bench.micro.data.StringGenerator.randShortDate;
import static com.neo4j.bench.micro.data.StringGenerator.randShortEmail;
import static com.neo4j.bench.micro.data.StringGenerator.randShortHex;
import static com.neo4j.bench.micro.data.StringGenerator.randShortLower;
import static com.neo4j.bench.micro.data.StringGenerator.randShortNumerical;
import static com.neo4j.bench.micro.data.StringGenerator.randShortUpper;
import static com.neo4j.bench.micro.data.StringGenerator.randShortUri;
import static com.neo4j.bench.micro.data.StringGenerator.randShortUtf8;
import static com.neo4j.bench.micro.data.StringGenerator.randUtf8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import static java.lang.String.format;

public class StringGeneratorTest
{
    private static final int REPETITIONS = 100_000;
    private static final long MAX_RANDOM_GENERATION_DURATION = TimeUnit.SECONDS.toMillis( 10 );
    private static final long MAX_ASCENDING_GENERATION_DURATION = TimeUnit.SECONDS.toMillis( 10 );

    @Test
    public void randomStringGeneratorsShouldDoNothingUnexpected() throws IOException
    {
        assertRandomStringGeneratorWorksAsAdvertised( randShortNumerical(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortDate(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortHex(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortLower(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortUpper(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortEmail(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortUri(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortAlphaNumerical(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortAlphaSymbolical(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randShortUtf8(), -1 );
        assertRandomStringGeneratorWorksAsAdvertised( randUtf8( BIG_STRING_LENGTH ), BIG_STRING_LENGTH );
    }

    @Test
    public void ascendingStringGeneratorsShouldDoNothingUnexpected() throws IOException
    {
        assertAscendingStringGeneratorWorksAsAdvertised(
                intString( ascInt( 0 ), BIG_STRING_LENGTH ), BIG_STRING_LENGTH );
        assertAscendingStringGeneratorWorksAsAdvertised(
                intString( ascInt( 0 ), SMALL_STRING_LENGTH ), SMALL_STRING_LENGTH );
    }

    @Test
    public void randomStringGeneratorsShouldBeDeterministic() throws IOException
    {
        assertGeneratorIsDeterministic( randShortNumerical(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortDate(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortDate(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortLower(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortUpper(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortEmail(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortUri(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortAlphaNumerical(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortAlphaSymbolical(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randShortUtf8(), MAX_RANDOM_GENERATION_DURATION );
        assertGeneratorIsDeterministic( randUtf8( BIG_STRING_LENGTH ), MAX_RANDOM_GENERATION_DURATION );
    }

    @Test
    public void ascendingStringGeneratorsShouldBeDeterministic() throws IOException
    {
        assertGeneratorIsDeterministic( intString( ascInt( 0 ), BIG_STRING_LENGTH ),
                                        MAX_ASCENDING_GENERATION_DURATION );
    }

    private void assertRandomStringGeneratorWorksAsAdvertised(
            ValueGeneratorFactory<String> stringGeneratorFactory,
            int expectedLength )
    {
        long start = System.currentTimeMillis();
        int repeatedValueCount = 0;
        SplittableRandom rng1 = RNGState.newRandom( 42L );
        SplittableRandom rng2 = RNGState.newRandom( 42L );
        ValueGeneratorFun<String> fun1 = stringGeneratorFactory.create();
        ValueGeneratorFun<String> fun2 = stringGeneratorFactory.create();
        String previous = fun1.next( rng1 );
        assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( previous ) ) );
        if ( -1 != expectedLength )
        {
            assertThat( previous.length(), equalTo( expectedLength ) );
        }
        for ( int i = 0; i < REPETITIONS; i++ )
        {
            String current = fun1.next( rng1 );
            assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( current ) ) );
            if ( current.equals( previous ) )
            {
                repeatedValueCount++;
            }
            if ( -1 != expectedLength )
            {
                assertThat( current.length(), equalTo( expectedLength ) );
            }
            previous = current;
        }
        long duration = System.currentTimeMillis() - start;
        int toleratedRepetitions = (int) (0.001 * REPETITIONS);
        System.out.println( format( "%s: Tolerated Repetitions = %s , Observed Repetitions = %s, Duration = %s (ms)",
                                    stringGeneratorFactory, toleratedRepetitions, repeatedValueCount, duration ) );
        assertThat( "less than 0.01% value repetitions", repeatedValueCount, lessThan( toleratedRepetitions ) );
        assertThat( duration, lessThan( MAX_RANDOM_GENERATION_DURATION ) );
    }

    private void assertAscendingStringGeneratorWorksAsAdvertised(
            ValueGeneratorFactory<String> stringGeneratorFactory,
            int expectedLength )
    {
        long start = System.currentTimeMillis();
        SplittableRandom rng1 = RNGState.newRandom( 42L );
        SplittableRandom rng2 = RNGState.newRandom( 42L );
        ValueGeneratorFun<String> fun1 = stringGeneratorFactory.create();
        ValueGeneratorFun<String> fun2 = stringGeneratorFactory.create();
        String previous = fun1.next( rng1 );
        assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( previous ) ) );
        if ( -1 != expectedLength )
        {
            assertThat( previous.length(), equalTo( expectedLength ) );
        }
        for ( int i = 0; i < REPETITIONS; i++ )
        {
            String current = fun1.next( rng1 );
            assertThat( fun2.nextValue( rng2 ), equalTo( Values.of( current ) ) );
            assertThat( current, greaterThan( previous ) );
            assertThat( Integer.parseInt( current ), greaterThan( Integer.parseInt( previous ) ) );
            if ( -1 != expectedLength )
            {
                assertThat( current.length(), equalTo( expectedLength ) );
            }
            previous = current;
        }
        long duration = System.currentTimeMillis() - start;
        System.out.println( format( "%s: Duration = %s (ms)", stringGeneratorFactory, duration ) );
        assertThat( duration, lessThan( MAX_ASCENDING_GENERATION_DURATION ) );
    }

    private void assertGeneratorIsDeterministic(
            ValueGeneratorFactory<String> generatorFactory, long toleratedDuration )
    {
        SplittableRandom rng1 = RNGState.newRandom( 42L );
        SplittableRandom rng2 = RNGState.newRandom( 42L );
        ValueGeneratorFun<String> fun1 = generatorFactory.create();
        ValueGeneratorFun<String> fun2 = generatorFactory.create();

        long start = System.currentTimeMillis();
        for ( int i = 0; i < REPETITIONS; i++ )
        {
            String value1 = fun1.next( rng1 );
            String value2 = fun2.next( rng2 );
            assertThat( value1, Matchers.equalTo( value2 ) );
        }
        long duration = System.currentTimeMillis() - start;
        System.out.println( format( "Duration = %s (ms)", duration ) );
        assertThat( duration, lessThan( toleratedDuration ) );
    }
}
