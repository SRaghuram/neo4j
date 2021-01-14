/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.neo4j.bench.data.DiscreteGenerator.Bucket;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static com.neo4j.bench.data.ConstantGenerator.constant;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_INL;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class DiscreteGeneratorTest
{
    private static final List<String> VALUE_TYPES = Arrays.asList(
            INT,
            LNG,
            FLT,
            DBL,
            STR_INL,
            STR_SML,
            STR_BIG,
            INT_ARR,
            LNG_ARR,
            FLT_ARR,
            DBL_ARR,
            STR_SML_ARR,
            STR_BIG_ARR,
            DATE_TIME,
            LOCAL_DATE_TIME,
            TIME,
            LOCAL_TIME,
            DATE,
            DURATION,
            POINT );

    @Test
    public void bucketsShouldBeEqualWhenTheirKeyAndValueAreEqual()
    {
        for ( String type : VALUE_TYPES )
        {
            assertThat( new Bucket( 1, constant( type, 1 ) ), equalTo( new Bucket( 1, constant( type, 1 ) ) ) );
        }
    }

    @Test
    public void bucketsShouldNotBeEqualWhenTheirKeysAreDifferent()
    {
        for ( String type : VALUE_TYPES )
        {
            assertThat( new Bucket( 1, constant( type, 1 ) ), not( equalTo( new Bucket( 2, constant( type, 1 ) ) ) ) );
        }
    }

    @Test
    public void bucketsShouldNotBeEqualWhenTheirValuesAreDifferent()
    {
        for ( String type : VALUE_TYPES )
        {
            assertThat( new Bucket( 1, constant( type, 1 ) ), not( equalTo( new Bucket( 1, constant( type, 2 ) ) ) ) );
        }
    }

    @Test
    public void bucketsShouldNotBeEqualWhenTheirValueTypesAreDifferent()
    {
        for ( String type : VALUE_TYPES )
        {
            for ( String otherType : VALUE_TYPES )
            {
                if ( !type.equals( otherType ) )
                {
                    assertThat( new Bucket( 1, constant( type, 1 ) ), not( equalTo( new Bucket( 1, constant( otherType, 1 ) ) ) ) );
                }
            }
        }
    }

    @Test
    public void shouldCreateExpectedDiscreteBucketSequence()
    {
        // for int
        doShouldCreateExpectedDiscreteBucketSequence( INT, 1, 2, 3, 1, 2, 3 );
        // for long
        doShouldCreateExpectedDiscreteBucketSequence( LNG, 7, 42, 1337, 7L, 42L, 1337L );
        // for double
        doShouldCreateExpectedDiscreteBucketSequence( DBL, 0.1, 0.01, 42, 0.1D, 0.01D, 42D );
        // for string
        doShouldCreateExpectedDiscreteBucketSequence( STR_SML, 1, 2, 1337, "1", "2", "1337" );
    }

    private <T> void doShouldCreateExpectedDiscreteBucketSequence(
            String type,
            Number value1,
            Number value2,
            Number value3,
            T expected1,
            T expected2,
            T expected3 )
    {
        int iterations = 1_000_000;
        double offsetConstant = 42;
        double value1Percentage = 0.1;
        double value2Percentage = 0.2;
        double value3Percentage = 0.7;

        SplittableRandom objectRng = new SplittableRandom( 42L );
        SplittableRandom valueRng = new SplittableRandom( 42L );
        Bucket bucket1 = new Bucket( value1Percentage * offsetConstant, constant( type, value1 ) );
        Bucket bucket2 = new Bucket( value2Percentage * offsetConstant, constant( type, value2 ) );
        Bucket bucket3 = new Bucket( value3Percentage * offsetConstant, constant( type, value3 ) );
        ValueGeneratorFun objectValues = DiscreteGenerator.discrete( bucket1, bucket2, bucket3 ).create();
        ValueGeneratorFun valueValues = DiscreteGenerator.discrete( bucket1, bucket2, bucket3 ).create();

        Map<Object,Integer> valueCounts = new HashMap<>();
        valueCounts.put( expected1, 0 );
        valueCounts.put( expected2, 0 );
        valueCounts.put( expected3, 0 );
        for ( int i = 0; i < iterations; i++ )
        {
            Object objectValue = objectValues.next( objectRng );
            Value valueValue = valueValues.nextValue( valueRng );
            assertThat( valueValue, equalTo( Values.of( objectValue ) ) );
            valueCounts.put( objectValue, valueCounts.get( objectValue ) + 1 );
        }

        double totalValueCounts = valueCounts.values().stream().mapToInt( Integer::intValue ).sum();
        assertWithinPercent( valueCounts.get( expected1 ) / totalValueCounts, value1Percentage, 0.01 );
        assertWithinPercent( valueCounts.get( expected2 ) / totalValueCounts, value2Percentage, 0.01 );
        assertWithinPercent( valueCounts.get( expected3 ) / totalValueCounts, value3Percentage, 0.01 );
    }

    private void assertWithinPercent( double value1, double value2, double maxPercent )
    {
        double actualPercent = Math.abs( 1 - value1 / value2 );
        assertThat( format( "%s was not within %s%% of %s", value1, maxPercent * 100, value2 ),
                actualPercent, lessThanOrEqualTo( maxPercent ) );
    }
}
