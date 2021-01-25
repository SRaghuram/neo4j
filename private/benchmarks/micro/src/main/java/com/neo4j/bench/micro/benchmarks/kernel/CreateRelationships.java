/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.NumberGenerator.stridingLong;
import static org.apache.commons.lang3.ArrayUtils.shuffle;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateRelationships extends AbstractKernelBenchmark
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

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return kernelImplementation;
    }

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
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( dense_node_threshold, denseNodeThreshold() )
                                          .withSetting( record_format, format )
                                          .build() )
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
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        long[] nodes;
        int type;
        int nodesPosition = -1;

        @Setup
        public void setUp( ThreadParams threadParams, CreateRelationships benchmarkState, RNGState rngState )
                throws InterruptedException, KernelException
        {
            initializeTx( benchmarkState, benchmarkState.txSize );
            int stride = threadParams.getThreadCount();
            boolean sliding = false;
            ValueGeneratorFun<Long> ids = stridingLong(
                    stride,
                    NODE_COUNT,
                    threadParams.getThreadIndex(),
                    sliding ).create();
            MutableLongList nodes = LongLists.mutable.empty();
            try ( Transaction tx = benchmarkState.db().beginTx() )
            {
                while ( !ids.wrapped() )
                {
                    nodes.add( ids.next( rngState.rng ) );
                }
            }
            this.nodes = nodes.toArray();
            // access store in random/scattered pattern
            // NOTE: really should use provided random, but shuffle does not support SplittableRandom
            shuffle( this.nodes, ThreadLocalRandom.current() );
            type = kernelTx.token.relationshipTypeGetOrCreateForName( TYPE.name() );
        }

        long nextNode()
        {
            nodesPosition = (nodesPosition + 1) % nodes.length;
            return nodes[nodesPosition];
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
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
    public long createRelationship( TxState txState ) throws KernelException
    {
        txState.kernelTx.advance();
        return txState.kernelTx.write.relationshipCreate( txState.nextNode(), txState.type, txState.nextNode() );
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateRelationships.class, methods );
    }
}
