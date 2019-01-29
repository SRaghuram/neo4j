package com.neo4j.bench.micro.data;

import org.junit.Test;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.neo4j.bench.micro.data.ConstantGenerator.BIG_STRING_PREFIX;
import static com.neo4j.bench.micro.data.ConstantGenerator.constant;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.SHORT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.SHORT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_INL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
    }

    private void assertConstant( Object expectedValue, ValueGeneratorFactory factory )
    {
        for ( int i = 0; i < 10; i++ )
        {
            // for a given factory the created functions is always the same
            ValueGeneratorFun fun = factory.create();
            for ( int j = 0; j < 10; j++ )
            {
                // for a given function the created values are always the same
                assertThat( expectedValue, equalTo( fun.next( null ) ) );
            }
        }
    }
}
