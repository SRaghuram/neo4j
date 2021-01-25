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
import com.neo4j.bench.micro.data.RelationshipDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class ReadById extends AbstractCoreBenchmark
{
    private static final int RELATIONSHIPS_PER_NODE = 1;
    private static final int NODE_COUNT = 1_000_000;
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE;
    private static final RelationshipDefinition RELATIONSHIP_DEFINITION =
            new RelationshipDefinition( RelationshipType.withName( "REL" ), RELATIONSHIPS_PER_NODE );

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
        return "Tests performance of retrieving nodes & relationships by ID.\n" +
               "Method:\n" +
               "- ID values are generated with uniform random policy";
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
                .withOutRelationships( RELATIONSHIP_DEFINITION )
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( record_format, format )
                                          .setTransactionMemory( txMemory )
                                          .build() )
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;

        @Setup
        public void setUp( ReadById benchmarkState ) throws InterruptedException
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
    public Node randomNodeById( TxState txState, RNGState rngState )
    {
        return txState.tx.getNodeById( rngState.rng.nextInt( NODE_COUNT ) );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public Relationship randomRelationshipById( TxState txState, RNGState rngState )
    {
        return txState.tx.getRelationshipById( rngState.rng.nextInt( RELATIONSHIP_COUNT ) );
    }
}
