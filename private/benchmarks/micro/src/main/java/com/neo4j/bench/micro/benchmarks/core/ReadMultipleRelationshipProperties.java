/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.RelationshipDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static com.google.common.collect.Iterables.size;
import static com.neo4j.bench.micro.data.DataGenerator.Order;
import static com.neo4j.bench.micro.data.DataGenerator.PropertyLocality;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;

@BenchmarkEnabled( false )
public class ReadMultipleRelationshipProperties extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 100_000;
    private static final RelationshipType TYPE = RelationshipType.withName( "REL" );
    private static final RelationshipDefinition RELATIONSHIPS_PER_NODE = new RelationshipDefinition( TYPE, 1 );
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE.count();
    private static final int PROPERTY_COUNT = 64;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"SCATTERED_BY_ELEMENT", "CO_LOCATED_BY_ELEMENT"},
            base = {"SCATTERED_BY_ELEMENT"} )
    @Param( {} )
    public PropertyLocality locality;

    private PropertyDefinition[] propertyDefinitions()
    {
        PropertyDefinition propertyDefinition = randPropertyFor( type );
        return IntStream.range( 0, PROPERTY_COUNT ).boxed()
                        .map( i -> propertyDefinition.key() + i )
                        .map( k -> new PropertyDefinition( k, propertyDefinition.value() ) )
                        .toArray( PropertyDefinition[]::new );
    }

    @Override
    public String description()
    {
        return "Tests performance of retrieving properties from relationships that have many properties.\n" +
               "Method:\n" +
               "- Every relationship has the same properties (with different values)\n" +
               "- On every relationship properties (keys) appear in the same order in the chain\n" +
               "- During store creation, property values are generated with uniform random policy\n" +
               "- When reading, relationship IDs are selected using two different policies: same, random\n" +
               "--- same: Same relationship accessed every time. Best cache hit rate. Test cached performance.\n" +
               "--- random: Accesses random relationships. Worst cache hit rate. Test non-cached performance.\n" +
               "- When reading, properties are selected using three different policies: first, half, all.\n" +
               "--- first: retrieve value for first property in chain\n" +
               "--- half: retrieve values for every property in the first half of the property chain\n" +
               "--- all: retrieve values for every property of the property chain\n" +
               "Outcome:\n" +
               "- Tests performance of property reading in cached & non-cached scenarios\n" +
               "- Tests performance of accessing different percentages of relationship property chain";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( RELATIONSHIPS_PER_NODE )
                .withRelationshipProperties( propertyDefinitions() )
                .withPropertyOrder( Order.ORDERED )
                .withPropertyLocality( locality )
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;
        String[] keysFirst;
        String[] keysHalf;
        String[] keysAll;

        @Setup
        public void setUp( ReadMultipleRelationshipProperties benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
            String[] propertyKeys = Stream.of( benchmarkState.propertyDefinitions() )
                                          .map( PropertyDefinition::key )
                                          .toArray( String[]::new );
            keysFirst = Arrays.copyOfRange( propertyKeys, 0, 1 );
            keysHalf = Arrays.copyOfRange( propertyKeys, 0, PROPERTY_COUNT / 2 );
            keysAll = Arrays.copyOfRange( propertyKeys, 0, PROPERTY_COUNT );
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    // --------------- SAME RELATIONSHIP ---------------

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public int sameRelationshipCountPropertyKeys( TxState txState )
    {
        return size( txState.tx.getRelationshipById( 1 ).getPropertyKeys() );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> sameRelationshipGetAllProperties( TxState txState )
    {
        return txState.tx.getRelationshipById( 1 ).getAllProperties();
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> sameRelationshipGetPropertiesFirst( TxState txState )
    {
        return txState.tx.getRelationshipById( 1 ).getProperties( txState.keysFirst );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> sameRelationshipGetPropertiesHalf( TxState txState )
    {
        return txState.tx.getRelationshipById( 1 ).getProperties( txState.keysHalf );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> sameRelationshipGetPropertiesAll( TxState txState )
    {
        return txState.tx.getRelationshipById( 1 ).getProperties( txState.keysAll );
    }

    // --------------- RANDOM RELATIONSHIP ---------------

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public int randomRelationshipCountPropertyKeys( TxState txState, RNGState rngState )
    {
        return size( txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) ).getPropertyKeys() );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> randomRelationshipGetAllProperties( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) ).getAllProperties();
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> randomRelationshipGetPropertiesFirst( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) )
                   .getProperties( txState.keysFirst );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> randomRelationshipGetPropertiesHalf( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) ).getProperties( txState.keysHalf );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public Map<String,Object> randomRelationshipGetPropertiesAll( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) ).getProperties( txState.keysAll );
    }
}
