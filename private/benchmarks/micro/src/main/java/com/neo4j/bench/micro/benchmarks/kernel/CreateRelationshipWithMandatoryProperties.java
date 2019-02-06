/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.RelationshipKeyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFun;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongList;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.Value;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.NumberGenerator.stridingLong;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.stridingFor;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

@BenchmarkEnabled( false )
public class CreateRelationshipWithMandatoryProperties extends AbstractKernelBenchmark
{
    private static final int NODE_COUNT = 1_000;
    private static final RelationshipType TYPE = RelationshipType.withName( "REL" );
    private static final String KEY = "key";
    private static final String NODES_ARE_NEVER_DENSE = Integer.toString( Integer.MAX_VALUE );

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String CreateRelationshipWithMandatoryProperties_type;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int CreateRelationshipWithMandatoryProperties_txSize;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation CreateRelationshipWithMandatoryProperties_kernel;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return CreateRelationshipWithMandatoryProperties_kernel;
    }

    @Override
    public String description()
    {
        return "Tests performance of creating relationships with mandatory constraints, " +
               "using different transaction batch sizes.\n" +
               "Method:\n" +
               "- Every node starts with zero relationships\n" +
               "- Threads work on node ID sequences\n" +
               "- Sequence of every thread is guaranteed to never overlap with that of another thread\n" +
               "- Threads choose random node pairs from their sequence then create a relationship between them\n" +
               "- All relationships are of the same type\n" +
               "- Threads create relationships, batching multiple writes per transaction\n" +
               "- Every created relationship has the same type\n" +
               "- Every created relationship has one property, always with same key, but different value\n" +
               "- There is a mandatory constraint on the type:property pair\n" +
               "Outcome:\n" +
               "- No two threads will never create a relationship on the same node (avoids deadlocks)\n" +
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
                .withNeo4jConfig(
                        // remove one variable. cost of creating (non) dense nodes is covered by another test
                        Neo4jConfig.empty().withSetting( dense_node_threshold, NODES_ARE_NEVER_DENSE ) )
                .withMandatoryRelationshipConstraints( new RelationshipKeyDefinition( TYPE, KEY ) )
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        long[] nodes;
        int nodesPosition = -1;
        ValueGeneratorFun values;
        int type;
        int key;

        @Setup
        public void setUp(
                ThreadParams threadParams,
                CreateRelationshipWithMandatoryProperties benchmarkState,
                RNGState rngState ) throws KernelException
        {
            initializeTx( benchmarkState, benchmarkState.CreateRelationshipWithMandatoryProperties_txSize );
            int stride = threadParams.getThreadCount();
            int offset = threadParams.getThreadIndex();
            // sequence should never wrap, 'sliding' value is irrelevant
            boolean sliding = false;
            values = stridingFor(
                    benchmarkState.CreateRelationshipWithMandatoryProperties_type,
                    Integer.MAX_VALUE,
                    stride,
                    offset,
                    sliding ).create();
            ValueGeneratorFun<Long> idFun = stridingLong( stride, NODE_COUNT, offset, sliding ).create();
            PrimitiveLongList nodeIds = Primitive.longList();
            long nodeId = idFun.next( rngState.rng );
            while ( !idFun.wrapped() )
            {
                nodeIds.add( nodeId );
                nodeId = idFun.next( rngState.rng );
            }
            nodes = nodeIds.toArray();
            // In threaded/concurrent scenario, each thread should access different parts of graph
            // NOTE: really should use provided random, but shuffle does not support SplittableRandom
            Collections.shuffle( Arrays.asList( nodes ), ThreadLocalRandom.current() );
            type = kernelTx.token.relationshipTypeGetOrCreateForName( TYPE.name() );
            key = kernelTx.token.propertyKeyGetOrCreateForName( KEY );
        }

        long nextNode()
        {
            nodesPosition = (nodesPosition + 1) % nodes.length;
            return nodes[nodesPosition];
        }

        Value value( SplittableRandom rng )
        {
            return values.nextValue( rng );
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
    public Value createRelationship( TxState txState, RNGState rngState ) throws KernelException
    {
        txState.kernelTx.advance();
        long relationship = txState.kernelTx.write.relationshipCreate( txState.nextNode(), txState.type, txState.nextNode() );
        return txState.kernelTx.write.relationshipSetProperty( relationship, txState.key, txState.value( rngState.rng ) );
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateRelationshipWithMandatoryProperties.class, methods );
    }
}
