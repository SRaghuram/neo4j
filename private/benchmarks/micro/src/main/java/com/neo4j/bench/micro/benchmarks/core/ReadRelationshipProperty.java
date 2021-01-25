/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
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

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

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
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( false )
public class ReadRelationshipProperty extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 1_000_000;
    private static final RelationshipType TYPE = RelationshipType.withName( "REL" );
    private static final RelationshipDefinition RELATIONSHIPS_PER_NODE = new RelationshipDefinition( TYPE, 1 );
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE.count();

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @Override
    public String description()
    {
        return "Tests performance of retrieving properties from relationships that have a single property.\n" +
               "Method:\n" +
               "- Every relationship has the same property (with different values)\n" +
               "- During store creation, property values are generated with uniform random policy\n" +
               "- When reading, relationship IDs are selected using two different policies: same, random\n" +
               "--- same: Same relationship accessed every time. Best cache hit rate. Test cached performance.\n" +
               "--- random: Accesses random relationship. Worst cache hit rate. Test non-cached performance.\n" +
               "Outcome:\n" +
               "- Tests performance of property reading in cached & non-cached scenarios";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        PropertyDefinition propertyDefinition = randPropertyFor( type );
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( RELATIONSHIPS_PER_NODE )
                .withRelationshipProperties( propertyDefinition )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().withSetting( record_format, format ).build() )
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;
        String propertyKey;

        @Setup
        public void setUp( ReadRelationshipProperty benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
            propertyKey = randPropertyFor( benchmarkState.type ).key();
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
    public Object sameRelationshipGetProperty( TxState txState )
    {
        return txState.tx.getRelationshipById( 1 ).getProperty( txState.propertyKey );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean sameRelationshipHasProperty( TxState txState )
    {
        return txState.tx.getRelationshipById( 1 ).hasProperty( txState.propertyKey );
    }

    // --------------- RANDOM RELATIONSHIP ---------------

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public Object randomRelationshipGetProperty( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) )
                   .getProperty( txState.propertyKey );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean randomRelationshipHasProperty( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) )
                   .hasProperty( txState.propertyKey );
    }
}
