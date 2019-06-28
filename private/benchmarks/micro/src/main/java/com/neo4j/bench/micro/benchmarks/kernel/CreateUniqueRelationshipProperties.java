/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.RelationshipDefinition;
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

import java.util.SplittableRandom;
import java.util.stream.IntStream;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.values.storable.Value;

import static com.neo4j.bench.micro.Main.run;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( false )
public class CreateUniqueRelationshipProperties extends AbstractKernelBenchmark
{
    private static final int NODE_COUNT = 10_000_000;
    private static final RelationshipType TYPE = RelationshipType.withName( "REL" );
    private static final RelationshipDefinition RELATIONSHIPS_PER_NODE = new RelationshipDefinition( TYPE, 1 );
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE.count();
    // Controls how many indexes are created
    private static final int MAX_PROPERTIES_PER_RELATIONSHIP = 10;
    private static final String[] KEYS = IntStream.range( 0, MAX_PROPERTIES_PER_RELATIONSHIP ).boxed()
                                                  .map( i -> "key" + i )
                                                  .toArray( String[]::new );

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String CreateUniqueRelationshipProperties_type;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int CreateUniqueRelationshipProperties_txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String CreateUniqueRelationshipProperties_format;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation CreateUniqueRelationshipProperties_kernel;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return CreateUniqueRelationshipProperties_kernel;
    }

    @Override
    public String description()
    {
        return "Tests performance of relationship property creation, using different transaction batch sizes.\n" +
               "Creates unique key:value property pairs.\n" +
               "Runs in two indexing scenarios: no index, schema index.\n" +
               "Guarantees unique values in the presence of parallelism:\n" +
               "- Every relationship has the same type\n" +
               "- Threads work on relationship ID sequences\n" +
               "- Sequence of every thread is guaranteed to never overlap with that of another thread\n" +
               "- Every thread starts at different offset (to accelerate warmup) in range, then wraps at max\n" +
               "- When a sequence wraps the thread moves onto the next property key\n" +
               "- Guarantees that for any property, relationship ID appears in the sequence of exactly one thread\n" +
               "- Guarantees property(key):relationship(id) uniqueness, so same policy used for property values\n" +
               "- I.e., value assigned to relationship property is ID of that relationship (in appropriate type)\n" +
               "Outcome:\n" +
               "- No two threads will ever create a property on the same relationship (avoids deadlocks)\n" +
               "- Every relationship will have the same properties\n" +
               "- No two relationships will have the same value for the same property";
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
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( record_format, CreateUniqueRelationshipProperties_format )
                                          .build() )
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        ValueGeneratorFun<Long> ids;
        int keyId;
        ValueGeneratorFun values;
        long relationshipId;
        int[] keys;

        @Setup
        public void setUp( ThreadParams threadParams, CreateUniqueRelationshipProperties benchmarkState ) throws KernelException
        {
            initializeTx( benchmarkState, benchmarkState.CreateUniqueRelationshipProperties_txSize );
            ids = nonContendingStridingFor(
                    LNG,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    RELATIONSHIP_COUNT ).create();
            values = nonContendingStridingFor(
                    benchmarkState.CreateUniqueRelationshipProperties_type,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    RELATIONSHIP_COUNT ).create();
            keyId = 0;
            relationshipId = -1;
            keys = propertyKeysToIds( KEYS );
        }

        long relationshipId()
        {
            return relationshipId;
        }

        int key()
        {
            return keys[keyId];
        }

        Value value( SplittableRandom rng )
        {
            return values.nextValue( rng );
        }

        Write advance( SplittableRandom rng ) throws InvalidTransactionTypeKernelException, TransactionFailureException
        {
            kernelTx.advance();
            relationshipId = ids.next( rng );
            if ( ids.wrapped() )
            {
                keyId++;
            }
            return kernelTx.write;
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
    public Value setProperty( TxState txState, RNGState rngState ) throws KernelException
    {
        Write write = txState.advance( rngState.rng );
        return write.relationshipSetProperty( txState.relationshipId(), txState.key(), txState.value( rngState.rng ) );
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateUniqueRelationshipProperties.class, methods );
    }
}
