/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.stream.IntStream;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.DataGenerator.LabelLocality;
import com.neo4j.bench.micro.data.DataGenerator.Order;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;

import static com.neo4j.bench.micro.Main.run;

@BenchmarkEnabled( true )
public class ReadLabel extends AbstractCoreBenchmark
{
    public static final int NODE_COUNT = 100_000;

    @ParamValues(
            allowed = {"4", "64"},
            base = {"64"} )
    @Param( {} )
    public int ReadLabel_count;

    @ParamValues(
            allowed = {"SCATTERED_BY_NODE", "CO_LOCATED_BY_NODE"},
            base = {"SCATTERED_BY_NODE"} )
    @Param( {} )
    public LabelLocality ReadLabel_locality;

    @ParamValues(
            allowed = {"off_heap", "on_heap"},
            base = {"on_heap"} )
    @Param( {} )
    public String ReadLabel_txMemory;

    @Override
    public String description()
    {
        return "Tests performance of retrieving labels from nodes that have many labels.\n" +
               "Method:\n" +
               "- Every node has the same labels\n" +
               "- For every node labels are added in the same order\n" +
               "- When looking up node to read labels for, node ID is selected using uniform random policy\n" +
               "- When reading labels on nodes, four different policies are used: first, last, random, all.\n" +
               "--- first: check for existence of label that was written to the node first\n" +
               "--- last: check for existence of label that was written to the node last\n" +
               "--- random: check for existence of random label from node\n" +
               "--- all: retrieve all labels from node";
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
                .withLabels( labels( ReadLabel_count ) )
                .withLabelOrder( Order.ORDERED )
                .withLabelLocality( ReadLabel_locality )
                .isReusableStore( true )
                .withNeo4jConfig( Neo4jConfig.empty().setTransactionMemory( ReadLabel_txMemory ) )
                .build();
    }

    public static Label[] labels( int count )
    {
        return IntStream.range( 0, count ).boxed()
                .map( i -> Label.label( "Label" + i ) )
                .toArray( Label[]::new );
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;
        Label[] labels;
        Label firstLabel;
        Label lastLabel;

        @Setup
        public void setUp( ReadLabel benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
            labels = labels( benchmarkState.ReadLabel_count );
            firstLabel = labels[0];
            lastLabel = labels[labels.length - 1];
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long countLabels( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return Iterables.count( db().getNodeById( nodeId ).getLabels() );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasRandomLabel( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomLabelIndex = rngState.rng.nextInt( txState.labels.length );
        Label label = txState.labels[randomLabelIndex];
        return db().getNodeById( nodeId ).hasLabel( label );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasFirstLabel( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return db().getNodeById( nodeId ).hasLabel( txState.firstLabel );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasLastLabel( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return db().getNodeById( nodeId ).hasLabel( txState.lastLabel );
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReadLabel.class, methods );
    }
}
