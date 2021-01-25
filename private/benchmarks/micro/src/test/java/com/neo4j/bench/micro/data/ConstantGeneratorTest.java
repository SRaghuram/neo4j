/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static com.neo4j.bench.micro.data.ConstantGenerator.BIG_STRING_PREFIX;
import static com.neo4j.bench.micro.data.ConstantGenerator.constant;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.SHORT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.SHORT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_INL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.TIME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static java.time.ZoneOffset.UTC;

import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;

public class ConstantGeneratorTest
{
    @Test
    public void shouldReturnExpectedValue()
    {
        assertConstant( (byte) 0, constant( BYTE, 0 ) );
        assertConstant( (short) 1, constant( SHORT, 1 ) );
        assertConstant( 2, constant( INT, 2 ) );
        assertConstant( 3L, constant( LNG, 3 ) );
        assertConstant( 4F, constant( FLT, 4 ) );
        assertConstant( 5D, constant( DBL, 5 ) );
        assertConstant( "in", constant( STR_INL, "in" ) );
        assertConstant( "6", constant( STR_SML, 6 ) );
        assertConstant( "word", constant( STR_SML, "word" ) );
        assertConstant( BIG_STRING_PREFIX + "7", constant( STR_BIG, 7 ) );
        assertConstant( BIG_STRING_PREFIX + "word", constant( STR_BIG, BIG_STRING_PREFIX + "word" ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> (byte) 8 ).toArray( Byte[]::new ),
                constant( BYTE_ARR, 8 ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> (short) 9 ).toArray( Short[]::new ),
                constant( SHORT_ARR, 9 ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).map( i -> 10 ).toArray(),
                constant( INT_ARR, 10 ) );
        assertConstant(
                LongStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).map( i -> 11L ).toArray(),
                constant( LNG_ARR, 11 ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> 12F ).toArray( Float[]::new ),
                constant( FLT_ARR, 12 ) );
        assertConstant(
                LongStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToDouble( i -> 13D ).toArray(),
                constant( DBL_ARR, 13 ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> "14" ).toArray( String[]::new ),
                constant( STR_SML_ARR, 14 ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> "word" ).toArray( String[]::new ),
                constant( STR_SML_ARR, "word" ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> BIG_STRING_PREFIX + "15" ).toArray( String[]::new ),
                constant( STR_BIG_ARR, 15 ) );
        assertConstant(
                IntStream.range( 0, ArrayGenerator.DEFAULT_SIZE ).mapToObj( i -> BIG_STRING_PREFIX + "word" ).toArray( String[]::new ),
                constant( STR_BIG_ARR, BIG_STRING_PREFIX + "word" ) );
        assertConstant( DateTimeValue.datetime( 16, 0, UTC ).asObjectCopy(), constant( DATE_TIME, 16 ) );
        assertConstant( LocalDateTimeValue.localDateTime( 17, 0 ).asObjectCopy(), constant( LOCAL_DATE_TIME, 17 ) );
        assertConstant( TimeValue.time( 18, UTC ).asObjectCopy(), constant( TIME, 18 ) );
        assertConstant( LocalTimeValue.localTime( 19 ).asObjectCopy(), constant( LOCAL_TIME, 19 ) );
        assertConstant( DateValue.epochDate( 20 ).asObjectCopy(), constant( DATE, 20 ) );
        assertConstant( DurationValue.duration( 0, 0, 0, 21 ).asObjectCopy(), constant( DURATION, 21 ) );
        assertConstant( Values.pointValue( Cartesian, 22, 22 ), constant( POINT, 22 ) );
    }

    private void assertConstant( Object expectedValue, ValueGeneratorFactory factory )
    {
        for ( int i = 0; i < 10; i++ )
        {
            // for a given factory the created functions is always the same
            ValueGeneratorFun fun1 = factory.create();
            ValueGeneratorFun fun2 = factory.create();
            for ( int j = 0; j < 10; j++ )
            {
                // for a given function the created values are always the same
                Object objectValue = fun1.next( null );
                Value valueValue = fun2.nextValue( null );
                assertThat( expectedValue, equalTo( objectValue ) );
                assertThat( valueValue, equalTo( Values.of( objectValue ) ) );
            }
        }
    }
}
