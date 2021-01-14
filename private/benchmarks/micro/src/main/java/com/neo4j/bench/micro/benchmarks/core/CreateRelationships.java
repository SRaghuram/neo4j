/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.Main;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.ValueGeneratorFun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.data.NumberGenerator.stridingLong;
import static org.apache.commons.lang3.ArrayUtils.shuffle;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateRelationships extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 1_000;
    private static final RelationshipType TYPE = RelationshipType.withName( "REL" );

    @ParamValues(
            allowed = {"1", "100", "10000"},
            base = {"100"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"true", "false"},
            base = {"true"} )
    @Param( {} )
    public boolean dense;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @Override
    public String description()
    {
        return "Tests performance of relationship creation, using different transaction batch sizes.\n" +
               "Method:\n" +
               "- Every node starts with zero relationships\n" +
               "- Threads work on node ID ranges\n" +
               "- Nodes ID range of every thread is guaranteed to never overlap with that of another thread\n" +
               "- Threads choose random node pairs from their range then create a relationship between them\n" +
               "- All relationships are of the same type\n" +
               "Outcome:\n" +
               "- No two threads will ever create a relationship on the same node (avoids deadlocks)\n" +
               "- Every node will have approximately the same number of relationships\n" +
               "- Relationships will be spread uniformly across all nodes";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.empty()
                                                    .withSetting( dense_node_threshold, denseNodeThreshold() )
                                                    .withSetting( record_format, format )
                                                    .build();
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( false )
                .build();
    }

    private String denseNodeThreshold()
    {
        return dense
               // HIGH dense node threshold --> NO nodes are dense
               ? Integer.toString( Integer.MAX_VALUE )
               // LOW dense node threshold --> ALL nodes are dense
               : "1";
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        long[] nodesIds;
        int nodesPosition = -1;

        @Setup
        public void setUp( ThreadParams threadParams, CreateRelationships benchmarkState, RNGState rngState )
                throws InterruptedException
        {
            int stride = threadParams.getThreadCount();
            boolean sliding = false;
            ValueGeneratorFun<Long> ids = stridingLong(
                    stride,
                    NODE_COUNT,
                    threadParams.getThreadIndex(),
                    sliding ).create();
            List<Node> nodesList = new ArrayList<>();
            try ( Transaction tx = benchmarkState.db().beginTx() )
            {
                while ( !ids.wrapped() )
                {
                    nodesList.add( tx.getNodeById( ids.next( rngState.rng ) ) );
                }
            }
            nodesIds = nodesList.stream().mapToLong( Node::getId ).toArray();
            // access store in random/scattered pattern
            // NOTE: really should use provided random, but shuffle does not support SplittableRandom
            shuffle( this.nodesIds, ThreadLocalRandom.current() );
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
        }

        Node nextNode()
        {
            nodesPosition = (nodesPosition + 1) % nodesIds.length;
            return txBatch.transaction().getNodeById( nodesIds[nodesPosition] );
        }

        void advance()
        {
            txBatch.advance();
        }

        @TearDown
        public void tearDown()
        {
            txBatch.close();
        }
    }

    /**
     * Note: Mode.SampleTime purposely not used in combination with transaction batching.
     * <p>
     * Reason: invocations containing a transaction commit will have very different latency profile, resulting in
     * deceptively low percentile values for invocations that do not commit, and vice versa for invocations that do.
     * Making sense of those plots will be difficult.
     */
    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public void createRelationship( TxState txState )
    {
        txState.advance();
        txState.nextNode().createRelationshipTo( txState.nextNode(), TYPE );
    }

    public static void main( String... methods ) throws Exception
    {
        Main.run( CreateRelationships.class, methods );
    }
}
