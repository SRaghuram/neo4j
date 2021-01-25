/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.RelationshipDefinition;
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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReadAll extends AbstractCoreBenchmark
{
    private static final int RELATIONSHIPS_PER_NODE = 1;
    public static final int NODE_COUNT = 10_000_000;
    public static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE;
    public static final RelationshipDefinition RELATIONSHIP_DEFINITION =
            new RelationshipDefinition( RelationshipType.withName( "REL" ), RELATIONSHIPS_PER_NODE );
    public static final Label LABEL = Label.label( "Label" );

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    @Override
    public String description()
    {
        return "Tests performance of iterating through all nodes/relationships.";
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
                .withLabels( LABEL )
                .withOutRelationships( RELATIONSHIP_DEFINITION )
                .isReusableStore( true )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().setTransactionMemory( txMemory ).build() )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;

        @Setup
        public void setUp( ReadAll benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabel( TxState txState, Blackhole bh )
    {
        assertCount( txState.tx.findNodes( LABEL ), NODE_COUNT, bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void allNodes( TxState txState, Blackhole bh )
    {
        assertCount( txState.tx.getAllNodes(), NODE_COUNT, bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void allRelationships( TxState txState, Blackhole bh )
    {
        assertCount( txState.tx.getAllRelationships(), RELATIONSHIP_COUNT, bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReadAll.class, methods );
    }
}
