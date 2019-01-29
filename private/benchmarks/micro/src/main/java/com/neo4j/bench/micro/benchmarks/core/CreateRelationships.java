package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.micro.data.NumberGenerator.stridingLong;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateRelationships extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 1_000;
    private static final RelationshipType TYPE = RelationshipType.withName( "REL" );

    @ParamValues(
            allowed = {"1", "100", "10000"},
            base = {"100"} )
    @Param( {} )
    public int CreateRelationships_txSize;

    @ParamValues(
            allowed = {"true", "false"},
            base = {"true"} )
    @Param( {} )
    public boolean CreateRelationships_dense;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String CreateRelationships_format;

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
        Neo4jConfig neo4jConfig = Neo4jConfig.empty()
                .withSetting( dense_node_threshold, denseNodeThreshold() )
                .withSetting( record_format, CreateRelationships_format );
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( false )
                .build();
    }

    private String denseNodeThreshold()
    {
        return CreateRelationships_dense
               // HIGH dense node threshold --> NO nodes are dense
               ? Integer.toString( Integer.MAX_VALUE )
               // LOW dense node threshold --> ALL nodes are dense
               : "1";
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        Node[] nodes;
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
            try ( Transaction ignore = benchmarkState.db().beginTx() )
            {
                while ( !ids.wrapped() )
                {
                    nodesList.add( benchmarkState.db().getNodeById( ids.next( rngState.rng ) ) );
                }
            }
            nodes = nodesList.toArray( new Node[nodesList.size()] );
            // access store in random/scattered pattern
            // NOTE: really should use provided random, but shuffle does not support SplittableRandom
            Collections.shuffle( Arrays.asList( nodes ), ThreadLocalRandom.current() );
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.CreateRelationships_txSize );
        }

        Node nextNode()
        {
            nodesPosition = (nodesPosition + 1) % nodes.length;
            return nodes[nodesPosition];
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
}
