/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.google.common.collect.Lists;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.calculateCumulativeSelectivities;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.makeSelectivityCumulative;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.middlePad;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.prefixPad;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.suffixPad;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValueGeneratorUtilTest
{
    @Test
    void shouldMakeSelectivityCumulative() throws IOException
    {
        List<Double> lowerSelectivities = Lists.newArrayList( 2D, 1D );
        Double targetSelectivity = 4D;
        Double cumulativeSelectivity = makeSelectivityCumulative( targetSelectivity, lowerSelectivities );
        assertThat( cumulativeSelectivity, equalTo( 1D ) );
    }

    @Test
    void shouldFailToMakeSelectivityCumulativeWhenTargetIsTooLow() throws IOException
    {
        List<Double> lowerSelectivities = Lists.newArrayList( 2D, 1D );
        Double targetSelectivity = 3D;
        assertThrows( Kaboom.class, () ->
        {
            makeSelectivityCumulative( targetSelectivity, lowerSelectivities );
        });
    }

    @Test
    void shouldFailToCalculateCumulativeSelectivitiesWhenInputIsNotInAscendingOrder() throws IOException
    {
        List<Double> originalSelectivities = Lists.newArrayList( 2D, 1D );
        assertThrows( Kaboom.class, () ->
        {
            calculateCumulativeSelectivities( originalSelectivities );
        });
    }

    @Test
    void shouldCalculateCumulativeSelectivities() throws IOException
    {
        List<Double> originalSelectivities = Lists.newArrayList( 1D, 2D, 4D );
        List<Double> cumulativeSelectivities = calculateCumulativeSelectivities( originalSelectivities );
        assertThat( cumulativeSelectivities, equalTo( Lists.newArrayList( 1D, 1D, 2D ) ) );
    }

    @Test
    void calculateCumulativeSelectivitiesShouldBeSortedInAscendingOrder() throws IOException
    {
        List<Double> originalSelectivities = Lists.newArrayList( 1D, 2D, 4D, 100D );
        List<Double> cumulativeSelectivities = calculateCumulativeSelectivities( originalSelectivities );
        for ( int i = 1; i < cumulativeSelectivities.size(); i++ )
        {
            assertTrue( cumulativeSelectivities.get( i ) >= cumulativeSelectivities.get( i - 1 ) );
        }
    }

    @Test
    void calculateCumulativeSelectivitiesOutputShouldBeSameSizeAsInput() throws IOException
    {
        List<Double> originalSelectivities = Lists.newArrayList( 1D, 2D, 4D, 100D );
        List<Double> cumulativeSelectivities = calculateCumulativeSelectivities( originalSelectivities );
        assertThat( originalSelectivities.size(), equalTo( cumulativeSelectivities.size() ) );
    }

    @Test
    void calculateCumulativeSelectivitiesOutputShouldNotCrash() throws IOException
    {
        List<Double> emptySelectivities = Lists.newArrayList();
        List<Double> singleSelectivities = Lists.newArrayList( 1D );
        List<Double> largeSelectivities = IntStream.range( 0, 10_000 ).boxed().map( Double::new ).collect( toList() );
        calculateCumulativeSelectivities( emptySelectivities );
        calculateCumulativeSelectivities( singleSelectivities );
        calculateCumulativeSelectivities( largeSelectivities );
    }

    @Test
    void shouldPrefixPad() throws IOException
    {
        String withPad = prefixPad( "12345", '0', 10 );
        assertThat( withPad, equalTo( "0000012345" ) );

        String withPadSmall = prefixPad( "12345", '0', 5 );
        assertThat( withPadSmall, is( isOneOf( "12345", "12345" ) ) );
    }

    @Test
    void shouldFailToPrefixPadWhenLengthTooShort() throws IOException
    {
        assertThrows( Kaboom.class, () ->
        {
            prefixPad( "12345", '0', 4 );
        });
    }

    @Test
    void shouldSuffixPad() throws IOException
    {
        String withPad = suffixPad( "12345", '0', 10 );
        assertThat( withPad, equalTo( "1234500000" ) );

        String withPadSmall = suffixPad( "12345", '0', 5 );
        assertThat( withPadSmall, is( isOneOf( "12345", "12345" ) ) );
    }

    @Test
    void shouldFailToSuffixPadWhenLengthTooShort() throws IOException
    {
        assertThrows( Kaboom.class, () ->
        {
            suffixPad( "12345", '0', 4 );
        });
    }

    @Test
    void shouldMiddlePad() throws IOException
    {
        String withPadRegular = middlePad( "12345", '0', 10 );
        assertThat( withPadRegular, is( isOneOf( "0012345000", "0001234500" ) ) );

        String withPadSmall = middlePad( "12345", '0', 6 );
        assertThat( withPadSmall, is( isOneOf( "012345", "123450" ) ) );
    }

    @Test
    void shouldFailToMiddlePadWhenLengthTooShort() throws IOException
    {
        assertThrows( Kaboom.class, () ->
        {
            middlePad( "12345", '0', 4 );
        });
    }
}
