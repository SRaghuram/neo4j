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
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.IndexType;
import com.neo4j.bench.data.LabelKeyDefinition;
import com.neo4j.bench.data.PropertyDefinition;
import com.neo4j.bench.data.ValueGeneratorFun;
import com.neo4j.bench.data.ValueGeneratorUtil.Range;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.SplittableRandom;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.data.ValueGeneratorUtil.defaultRangeFor;
import static com.neo4j.bench.data.ValueGeneratorUtil.randPropertyFor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class FindNodeUnique extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final int NODE_COUNT = 1_000_000;

    /*
    NOTE: POINT is not enabled because, at present, nodes are filled using <ascending> policy and queried using <random> policy.
          For other generators this covers the same space. For POINT <random> has larger space than <ascending>, therefore queries return no matching value.
     */
    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"NONE", "SCHEMA", "UNIQUE"},
            base = {"NONE", "SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label and property.\n" +
               "Runs in three indexing scenarios: no index, schema index, unique constraint.\n" +
               "Method:\n" +
               "- Every node has exactly one, same label\n" +
               "- Every node has exactly one property, same and the property (key)\n" +
               "- During store creation, property values are assigned with ascending policy, to guaranty uniqueness\n" +
               "- When reading, property value generation is uniform random, to spread load across store\n" +
               "Outcome:\n" +
               "- Accesses are spread evenly across the store\n" +
               "- No two nodes will have the same property value (allows for testing with unique constraint)";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Number initial = defaultRangeFor( type ).min();
        // will create sequence of ascending number values, starting at 'min' and ending at 'min' + NODE_COUNT
        PropertyDefinition propertyDefinition = ascPropertyFor( type, initial );
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().setTransactionMemory( txMemory ).build() )
                .isReusableStore( true );
        switch ( index )
        {
        case SCHEMA:
            return builder.withSchemaIndexes( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) ).build();
        case UNIQUE:
            return builder.withUniqueConstraints( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) ).build();
        case NONE:
            return builder.build();
        default:
            throw new RuntimeException( "Unsupported 'index' value: " + index );
        }
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;
        String propertyKey;
        ValueGeneratorFun valueFun;

        @Setup
        public void setUp( FindNodeUnique benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
            Range range = defaultRangeFor( benchmarkState.type );
            PropertyDefinition propertyDefinition = randPropertyFor(
                    benchmarkState.type,
                    range.min(),
                    range.min().longValue() + NODE_COUNT,
                    // numerical
                    true );
            propertyKey = propertyDefinition.key();
            valueFun = propertyDefinition.value().create();
        }

        Object nextValue( SplittableRandom rng )
        {
            return valueFun.next( rng );
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void findNodeByLabelKeyValue( TxState txState, RNGState rngState, Blackhole bh )
    {
        assertNotNull(
                txState.tx.findNode( LABEL, txState.propertyKey, txState.nextValue( rngState.rng ) ),
                bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValue( TxState txState, RNGState rngState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, txState.nextValue( rngState.rng ) ),
                1,
                bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( FindNodeUnique.class, methods );
    }
}
