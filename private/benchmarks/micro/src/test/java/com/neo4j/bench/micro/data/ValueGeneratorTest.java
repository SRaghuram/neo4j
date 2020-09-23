/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket;
import com.neo4j.bench.micro.data.PointGenerator.ClusterGridDefinition;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.SplittableRandom;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.micro.data.ArrayGenerator.DEFAULT_SIZE;
import static com.neo4j.bench.micro.data.ArrayGenerator.doubleArray;
import static com.neo4j.bench.micro.data.ArrayGenerator.floatArray;
import static com.neo4j.bench.micro.data.ArrayGenerator.intArray;
import static com.neo4j.bench.micro.data.ArrayGenerator.longArray;
import static com.neo4j.bench.micro.data.ArrayGenerator.stringArray;
import static com.neo4j.bench.micro.data.ConstantGenerator.constant;
import static com.neo4j.bench.micro.data.DiscreteGenerator.discrete;
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
import static com.neo4j.bench.micro.data.PointGenerator.circleGrid;
import static com.neo4j.bench.micro.data.PointGenerator.clusterGrid;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

@TestDirectoryExtension
public class ValueGeneratorTest
{
    @Inject
    public TestDirectory temporaryFolder;

    private static final int SAMPLE_SIZE = 10_000;

    @Test
    public void stringGeneratorsShouldDoEqualityCorrectly() throws IOException
    {
        assertThat( intString( ascInt( 0 ), BIG_STRING_LENGTH ),
                    equalTo( intString( ascInt( 0 ), BIG_STRING_LENGTH ) ) );
        assertThat( intString( randInt( 0, Integer.MAX_VALUE ), BIG_STRING_LENGTH ),
                    equalTo( intString( randInt( 0, Integer.MAX_VALUE ), BIG_STRING_LENGTH ) ) );
        assertThat( randShortNumerical(), equalTo( randShortNumerical() ) );
        assertThat( randShortDate(), equalTo( randShortDate() ) );
        assertThat( randShortHex(), equalTo( randShortHex() ) );
        assertThat( randShortLower(), equalTo( randShortLower() ) );
        assertThat( randShortUpper(), equalTo( randShortUpper() ) );
        assertThat( randShortEmail(), equalTo( randShortEmail() ) );
        assertThat( randShortUri(), equalTo( randShortUri() ) );
        assertThat( randShortAlphaNumerical(), equalTo( randShortAlphaNumerical() ) );
        assertThat( randShortAlphaSymbolical(), equalTo( randShortAlphaSymbolical() ) );
        assertThat( randShortUtf8(), equalTo( randShortUtf8() ) );
        assertThat( randUtf8( BIG_STRING_LENGTH ), equalTo( randUtf8( BIG_STRING_LENGTH ) ) );
    }

    @Test
    public void nonContendingStridingForMustNotOverlap()
    {
        HashSet<Long> setA = new HashSet<>();
        HashSet<Long> setB = new HashSet<>();
        HashSet<Long> setC = new HashSet<>();
        ValueGeneratorFun<Long> generatorA = nonContendingStridingFor( LNG, 4, 0, 100 ).create();
        ValueGeneratorFun<Long> generatorB = nonContendingStridingFor( LNG, 4, 1, 100 ).create();
        ValueGeneratorFun<Long> generatorC = nonContendingStridingFor( LNG, 4, 2, 100 ).create();
        ValueGeneratorFun<Long> generatorD = nonContendingStridingFor( LNG, 4, 3, 100 ).create();
        SplittableRandom rng = new SplittableRandom();

        for ( int i = 0; i < SAMPLE_SIZE; i++ )
        {
            setA.add( generatorA.next( rng ) );
        }
        for ( int i = 0; i < SAMPLE_SIZE; i++ )
        {
            long next = generatorB.next( rng );
            assertFalse( setA.contains( next ) );
            setB.add( next );
        }
        for ( int i = 0; i < SAMPLE_SIZE; i++ )
        {
            long next = generatorC.next( rng );
            assertFalse( setA.contains( next ) );
            assertFalse( setB.contains( next ) );
            setC.add( next );
        }
        for ( int i = 0; i < SAMPLE_SIZE; i++ )
        {
            long next = generatorD.next( rng );
            assertFalse( setA.contains( next ) );
            assertFalse( setB.contains( next ) );
            assertFalse( setC.contains( next ) );
        }
    }

    @Test
    public void stringGeneratorsShouldDoInequalityCorrectly() throws IOException
    {
        // when string length is different with ascending
        assertThat( intString( ascInt( 0 ), 1 ), not( equalTo( intString( ascInt( 0 ), 2 ) ) ) );
        // when string length is different with random
        assertThat( intString( randInt( 0, 10 ), 2 ), not( equalTo( intString( randInt( 0, 10 ), 3 ) ) ) );
        // when inner number generator has different parameters
        assertThat( intString( randInt( 0, 10 ), 2 ), not( equalTo( intString( randInt( 0, 11 ), 2 ) ) ) );
        // when inner number generator is of different type
        assertThat( intString( randInt( 0, 10 ), 2 ), not( equalTo( intString( ascInt( 0 ), 2 ) ) ) );
        // when string generator type is different
        assertThat( randShortNumerical(), not( equalTo( randShortAlphaNumerical() ) ) );
        // when string generator type is different
        assertThat( randShortUtf8(), not( equalTo( randUtf8( 54 ) ) ) );
        // when string is of different length
        assertThat( randUtf8( 2 ), not( equalTo( randUtf8( 3 ) ) ) );
    }

    @Test
    public void numberGeneratorsShouldDoEqualityCorrectly() throws IOException
    {
        assertThat( randInt( 0, Integer.MAX_VALUE ), equalTo( randInt( 0, Integer.MAX_VALUE ) ) );
        assertThat( randLong( 0, Long.MAX_VALUE ), equalTo( randLong( 0, Long.MAX_VALUE ) ) );
        assertThat( randFloat( 0, Float.MAX_VALUE ), equalTo( randFloat( 0, Float.MAX_VALUE ) ) );
        assertThat( randDouble( 0, Double.MAX_VALUE ), equalTo( randDouble( 0, Double.MAX_VALUE ) ) );
        assertThat( toFloat( randInt( 0, Integer.MAX_VALUE ) ),
                    equalTo( toFloat( randInt( 0, Integer.MAX_VALUE ) ) ) );
        assertThat( toDouble( randLong( 0, Long.MAX_VALUE ) ),
                    equalTo( toDouble( randLong( 0, Long.MAX_VALUE ) ) ) );
        assertThat( ascInt( 0 ), equalTo( ascInt( 0 ) ) );
        assertThat( ascLong( 0 ), equalTo( ascLong( 0 ) ) );
        assertThat( ascFloat( 0 ), equalTo( ascFloat( 0 ) ) );
        assertThat( ascDouble( 0 ), equalTo( ascDouble( 0 ) ) );
        assertThat( toFloat( ascInt( 0 ) ), equalTo( toFloat( ascInt( 0 ) ) ) );
        assertThat( toDouble( ascLong( 0 ) ), equalTo( toDouble( ascLong( 0 ) ) ) );
        int stride = 5;
        int max = 1_000;
        int offset = 7;
        boolean sliding = true;
        assertThat( stridingInt( stride, max, offset, sliding ),
                    equalTo( stridingInt( stride, max, offset, sliding ) ) );
        assertThat( stridingLong( stride, max, offset, sliding ),
                    equalTo( stridingLong( stride, max, offset, sliding ) ) );
        assertThat( stridingFloat( stride, max, offset, sliding ),
                    equalTo( stridingFloat( stride, max, offset, sliding ) ) );
        assertThat( stridingDouble( stride, max, offset, sliding ),
                    equalTo( stridingDouble( stride, max, offset, sliding ) ) );
        assertThat( toFloat( stridingInt( stride, max, offset, sliding ) ),
                    equalTo( toFloat( stridingInt( stride, max, offset, sliding ) ) ) );
        assertThat( toDouble( stridingLong( stride, max, offset, sliding ) ),
                    equalTo( toDouble( stridingLong( stride, max, offset, sliding ) ) ) );
    }

    @Test
    public void numberGeneratorsShouldDoInequalityCorrectly() throws IOException
    {
        // when generator type is the same but parameters are different
        assertThat( randInt( 0, 10 ), not( equalTo( randInt( 0, 11 ) ) ) );
        assertThat( randLong( 0, 10 ), not( equalTo( randLong( 0, 11 ) ) ) );
        assertThat( randFloat( 0, 10 ), not( equalTo( randFloat( 0, 11 ) ) ) );
        assertThat( randDouble( 0, 10 ), not( equalTo( randDouble( 0, 11 ) ) ) );

        assertThat( toFloat( randInt( 0, 10 ) ), not( equalTo( toFloat( randInt( 0, 11 ) ) ) ) );
        assertThat( toDouble( randLong( 0, 10 ) ), not( equalTo( toDouble( randLong( 0, 11 ) ) ) ) );
        assertThat( toFloat( randInt( 0, 10 ) ), not( equalTo( toDouble( randInt( 0, 10 ) ) ) ) );

        // when generator type is different
        assertThat( randInt( 0, 10 ), not( equalTo( randLong( 0, 10 ) ) ) );
        assertThat( randInt( 0, 10 ), not( equalTo( randFloat( 0, 10 ) ) ) );
        assertThat( randInt( 0, 10 ), not( equalTo( randDouble( 0, 10 ) ) ) );

        assertThat( toFloat( randInt( 0, 10 ) ), not( equalTo( toFloat( randLong( 0, 10 ) ) ) ) );
        assertThat( toFloat( randInt( 0, 10 ) ), not( equalTo( toFloat( randFloat( 0, 10 ) ) ) ) );

        // when generator type is the same but parameters are different
        assertThat( ascInt( 0 ), not( equalTo( ascInt( 1 ) ) ) );
        assertThat( ascLong( 0 ), not( equalTo( ascLong( 1 ) ) ) );
        assertThat( ascFloat( 0 ), not( equalTo( ascFloat( 1 ) ) ) );
        assertThat( ascDouble( 0 ), not( equalTo( ascDouble( 1 ) ) ) );

        // when generator type is different
        assertThat( ascInt( 0 ), not( equalTo( ascLong( 0 ) ) ) );
        assertThat( ascInt( 0 ), not( equalTo( ascFloat( 0 ) ) ) );
        assertThat( ascInt( 0 ), not( equalTo( ascDouble( 0 ) ) ) );

        // when generator type is the same but parameters are different
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingInt( 1, 10, 1, false ) ) ) );
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingInt( 2, 10, 1, true ) ) ) );
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingInt( 1, 11, 1, true ) ) ) );
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingInt( 1, 10, 2, true ) ) ) );

        // when generator type is different
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingLong( 1, 10, 1, true ) ) ) );
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingFloat( 1, 10, 1, true ) ) ) );
        assertThat( stridingInt( 1, 10, 1, true ), not( equalTo( stridingDouble( 1, 10, 1, true ) ) ) );
    }

    @Test
    public void arrayGeneratorsShouldDoEqualityCorrectly() throws IOException
    {
        assertThat( stringArray( intString( ascInt( 0 ), SMALL_STRING_LENGTH ), DEFAULT_SIZE ),
                    equalTo( stringArray( intString( ascInt( 0 ), SMALL_STRING_LENGTH ), DEFAULT_SIZE ) ) );
        assertThat( stringArray( randUtf8( SMALL_STRING_LENGTH ), DEFAULT_SIZE ),
                    equalTo( stringArray( randUtf8( SMALL_STRING_LENGTH ), DEFAULT_SIZE ) ) );
        assertThat( intArray( ascInt( 0 ), DEFAULT_SIZE ),
                    equalTo( intArray( ascInt( 0 ), DEFAULT_SIZE ) ) );
        assertThat( longArray( ascLong( 0 ), DEFAULT_SIZE ),
                    equalTo( longArray( ascLong( 0 ), DEFAULT_SIZE ) ) );
        assertThat( floatArray( ascFloat( 0 ), DEFAULT_SIZE ),
                    equalTo( floatArray( ascFloat( 0 ), DEFAULT_SIZE ) ) );
        assertThat( doubleArray( ascDouble( 0 ), DEFAULT_SIZE ),
                    equalTo( doubleArray( ascDouble( 0 ), DEFAULT_SIZE ) ) );
    }

    @Test
    public void arrayGeneratorsShouldDoInequalityCorrectly() throws IOException
    {
        // generators are of same type but arrays are of different length
        assertThat( stringArray( intString( ascInt( 0 ), 2 ), 5 ),
                    not( equalTo( stringArray( intString( ascInt( 0 ), 2 ), 6 ) ) ) );

        // generators are of same type but inner generator has different parameters
        assertThat( stringArray( intString( ascInt( 0 ), 2 ), 5 ),
                    not( equalTo( stringArray( intString( ascInt( 0 ), 3 ), 5 ) ) ) );

        // generators are of same type but deepest level inner generator has different parameters
        assertThat( stringArray( intString( ascInt( 0 ), 2 ), 5 ),
                    not( equalTo( stringArray( intString( ascInt( 1 ), 2 ), 5 ) ) ) );
        assertThat( intArray( ascInt( 0 ), 5 ), not( equalTo( longArray( ascLong( 0 ), 5 ) ) ) );
        assertThat( intArray( ascInt( 0 ), 5 ), not( equalTo( floatArray( ascFloat( 0 ), 5 ) ) ) );
        assertThat( intArray( ascInt( 0 ), 5 ), not( equalTo( doubleArray( ascDouble( 0 ), 5 ) ) ) );

        // generators are of same type with exception of deepest level inner array
        assertThat( stringArray( intString( ascInt( 0 ), 2 ), 5 ),
                    not( equalTo( stringArray( intString( randInt( 0, 10 ), 2 ), 5 ) ) ) );
    }

    @Test
    public void discreteGeneratorsShouldDoEqualityCorrectly() throws IOException
    {
        assertThat(
                discrete(
                        new Bucket( 1, constant( INT, 1 ) ),
                        new Bucket( 2L, constant( LNG, 2 ) ),
                        new Bucket( 3F, constant( FLT, 3 ) ),
                        new Bucket( 4D, constant( INT, 4 ) ),
                        new Bucket( 5, constant( STR_SML, 5 ) ) ),
                equalTo( discrete(
                        new Bucket( 1, constant( INT, 1 ) ),
                        new Bucket( 2L, constant( LNG, 2 ) ),
                        new Bucket( 3F, constant( FLT, 3 ) ),
                        new Bucket( 4D, constant( INT, 4 ) ),
                        new Bucket( 5, constant( STR_SML, 5 ) ) ) ) );
    }

    @Test
    public void discreteGeneratorsShouldDoInequalityCorrectly() throws IOException
    {
        // type of one bucket value is different
        assertThat(
                discrete(
                        new Bucket( 1, constant( INT, 1 ) ),
                        new Bucket( 2L, constant( LNG, 2 ) ),
                        new Bucket( 3F, constant( FLT, 3 ) ),
                        new Bucket( 4D, constant( DBL, 4 ) ),
                        new Bucket( 5, constant( STR_SML, 5 ) ) ),
                not( equalTo( discrete(
                        new Bucket( 1, constant( INT, 1 ) ),
                        new Bucket( 2L, constant( LNG, 2 ) ),
                        new Bucket( 3F, constant( FLT, 3 ) ),
                        new Bucket( 4D, constant( INT, 4 ) ),
                        new Bucket( 5, constant( STR_SML, 5 ) ) ) ) ) );

        // bucket list is shorter
        assertThat(
                discrete(
                        new Bucket( 1, constant( INT, 1 ) ),
                        new Bucket( 2L, constant( LNG, 2 ) ),
                        new Bucket( 3F, constant( FLT, 3 ) ),
                        new Bucket( 4D, constant( DBL, 4 ) ),
                        new Bucket( 5, constant( STR_SML, 5 ) ) ),
                not( equalTo( discrete(
                        new Bucket( 1, constant( INT, 1 ) ),
                        new Bucket( 2L, constant( LNG, 2 ) ),
                        new Bucket( 3F, constant( FLT, 3 ) ),
                        new Bucket( 4D, constant( DBL, 4 ) ) ) ) ) );
    }

    @Test
    public void shouldSerializeDeserializeStringGenerators() throws IOException
    {
        doShouldBeSerializable( intString( ascInt( 0 ), BIG_STRING_LENGTH ) );
        doShouldBeSerializable( intString( randInt( 0, Integer.MAX_VALUE ), BIG_STRING_LENGTH ) );
        doShouldBeSerializable( randShortNumerical() );
        doShouldBeSerializable( randShortDate() );
        doShouldBeSerializable( randShortHex() );
        doShouldBeSerializable( randShortLower() );
        doShouldBeSerializable( randShortUpper() );
        doShouldBeSerializable( randShortEmail() );
        doShouldBeSerializable( randShortUri() );
        doShouldBeSerializable( randShortAlphaNumerical() );
        doShouldBeSerializable( randShortAlphaSymbolical() );
        doShouldBeSerializable( randShortUtf8() );
        doShouldBeSerializable( randUtf8( BIG_STRING_LENGTH ) );
    }

    @Test
    public void shouldSerializableDeserializeNumberGenerators() throws IOException
    {
        doShouldBeSerializable( randInt( 0, Integer.MAX_VALUE ) );
        doShouldBeSerializable( randLong( 0, Long.MAX_VALUE ) );
        doShouldBeSerializable( randFloat( 0, Float.MAX_VALUE ) );
        doShouldBeSerializable( randDouble( 0, Double.MAX_VALUE ) );
        doShouldBeSerializable( ascInt( 0 ) );
        doShouldBeSerializable( ascLong( 0 ) );
        doShouldBeSerializable( ascFloat( 0 ) );
        doShouldBeSerializable( ascDouble( 0 ) );
        doShouldBeSerializable( toFloat( ascInt( 0 ) ) );
        doShouldBeSerializable( toDouble( ascLong( 0 ) ) );

        int stride = 5;
        int max = 1_000;
        int offset = 7;
        boolean sliding = true;
        doShouldBeSerializable( stridingInt( stride, max, offset, sliding ) );
        doShouldBeSerializable( stridingLong( stride, max, offset, sliding ) );
        doShouldBeSerializable( stridingFloat( stride, max, offset, sliding ) );
        doShouldBeSerializable( stridingDouble( stride, max, offset, sliding ) );
    }

    @Test
    public void shouldSerializableDeserializeArrayGenerators() throws IOException
    {
        doShouldBeSerializable( stringArray( intString( ascInt( 0 ), SMALL_STRING_LENGTH ), DEFAULT_SIZE ) );
        doShouldBeSerializable( stringArray( randUtf8( SMALL_STRING_LENGTH ), DEFAULT_SIZE ) );
        doShouldBeSerializable( intArray( ascInt( 0 ), DEFAULT_SIZE ) );
        doShouldBeSerializable( longArray( ascLong( 0 ), DEFAULT_SIZE ) );
        doShouldBeSerializable( floatArray( ascFloat( 0 ), DEFAULT_SIZE ) );
        doShouldBeSerializable( doubleArray( ascDouble( 0 ), DEFAULT_SIZE ) );
    }

    @Test
    public void shouldSerializableDeserializeDiscreteGenerators() throws IOException
    {
        doShouldBeSerializable( discrete(
                new Bucket( 1, constant( INT, 1 ) ),
                new Bucket( 2L, constant( LNG, 2 ) ),
                new Bucket( 3F, constant( FLT, 3 ) ),
                new Bucket( 4D, constant( DBL, 4 ) ),
                new Bucket( 5, constant( STR_SML, 5 ) ) ) );
        doShouldBeSerializable( discrete(
                new Bucket( 1, constant( INT_ARR, 1 ) ),
                new Bucket( 2L, constant( LNG_ARR, 2 ) ),
                new Bucket( 3F, constant( FLT_ARR, 3 ) ),
                new Bucket( 4D, constant( DBL_ARR, 4 ) ),
                new Bucket( 5, constant( STR_SML_ARR, 5 ) ) ) );
    }

    @Test
    public void shouldSerializableDeserializeConstantGenerators() throws Exception
    {
        doShouldBeSerializable( constant( STR_SML, 1 ) );
        doShouldBeSerializable( constant( LNG, 1 ) );
    }

    @Test
    public void shouldSerializeDeserializePointGenerators() throws IOException
    {
        doShouldBeSerializable( PointGenerator.random( -1, 1, Long.MIN_VALUE, Long.MAX_VALUE, new CRS.Cartesian() ) );
        doShouldBeSerializable( PointGenerator.random( -1, 1, -90, 90, new CRS.WGS84() ) );
        doShouldBeSerializable( PointGenerator.grid( -10, 10, 0, 100, 1_000, new CRS.Cartesian() ) );
        doShouldBeSerializable( PointGenerator.grid( -180, 180, -90, 90, 1_000, new CRS.WGS84() ) );
        doShouldBeSerializable( circleGrid( ClusterGridDefinition.from(
                10,
                10,
                10,
                10,
                -1_000,
                1_000,
                0,
                10_000,
                1_000_000,
                new CRS.Cartesian()
        ) ) );
        doShouldBeSerializable( circleGrid( ClusterGridDefinition.from(
                10,
                10,
                -1_000,
                1_000,
                -90,
                90,
                1_000_000,
                new CRS.WGS84()
        ) ) );
        doShouldBeSerializable( clusterGrid( ClusterGridDefinition.from(
                10,
                10,
                10,
                10,
                -1_000,
                1_000,
                0,
                10_000,
                1_000_000,
                new CRS.Cartesian()
        ) ) );
        doShouldBeSerializable( clusterGrid( ClusterGridDefinition.from(
                10,
                10,
                -1_000,
                1_000,
                -90,
                90,
                1_000_000,
                new CRS.WGS84()
        ) ) );
    }

    private void doShouldBeSerializable( ValueGeneratorFactory factory ) throws IOException
    {
        SplittableRandom rng = new SplittableRandom( 42L );
        ValueGeneratorFun fun = factory.create();
        List values = new ArrayList();
        for ( int i = 0; i < SAMPLE_SIZE; i++ )
        {
            values.add( fun.next( rng ) );
        }

        File jsonFile = temporaryFolder.file( "file.json" ).toFile();
        JsonUtil.serializeJson( jsonFile.toPath(), factory );
        ValueGeneratorFactory factoryAfter = JsonUtil.deserializeJson( jsonFile.toPath(), factory.getClass() );

        SplittableRandom rngAfter = new SplittableRandom( 42L );
        ValueGeneratorFun funAfter = factoryAfter.create();
        List valuesAfter = new ArrayList();
        for ( int i = 0; i < SAMPLE_SIZE; i++ )
        {
            valuesAfter.add( funAfter.next( rngAfter ) );
        }

        for ( int i = 0; i < values.size(); i++ )
        {
            assertThat( values.get( i ), equalTo( valuesAfter.get( i ) ) );
        }
    }
}
