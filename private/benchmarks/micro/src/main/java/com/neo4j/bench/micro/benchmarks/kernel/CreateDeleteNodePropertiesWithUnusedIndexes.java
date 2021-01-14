/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.data.DataGenerator.Order;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.LabelKeyDefinition;
import com.neo4j.bench.data.PropertyDefinition;
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

import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.values.storable.Value;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.nonContendingStridingFor;
import static com.neo4j.bench.data.ValueGeneratorUtil.randPropertyFor;

@BenchmarkEnabled( true )
public class CreateDeleteNodePropertiesWithUnusedIndexes extends AbstractKernelBenchmark
{
    private static final String UNUSED_KEY_PREFIX = "unused_key_";
    private static final int NODE_COUNT = 100_000;

    @ParamValues(
            allowed = {"1", "10", "100", "1000"},
            base = {"1"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {"2"},
            base = {"2"} )
    @Param( {} )
    public int propertyCount;

    @ParamValues(
            allowed = {"10", "100", "1000", "10000"},
            base = {"100", "10000"} )
    @Param( {} )
    public int indexCount;

    @ParamValues(
            allowed = {"1", "3", "10", "100", "10000"},
            base = {"3"} )
    @Param( {} )
    public int indexCountPerLabel;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG} )
    @Param( {} )
    public String type;

    private static final String NONE = "NONE", INDEXED = "INDEXED", UNINDEXED = "UNINDEXED";
    @ParamValues(
            allowed = {NONE, INDEXED, UNINDEXED},
            base = {NONE, INDEXED, UNINDEXED} )
    @Param( {} )
    public String label;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernel;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return kernel;
    }

    /**
     * - Threads work on node ID sequences
     * - Sequence of every thread is guaranteed to never overlap with that of another thread
     * - Every thread starts at different offset (to accelerate warmup) in range, then wraps at max
     * - At sequence beginning threads add/remove properties at specific offsets of 'property chain'
     * - At each following node (in sequence) the offsets to add/remove from are incremented by one
     * - When a sequence wraps the add/remove offsets are reset to their initial values, plus one.
     * This guarantees that every node has exactly one property 'missing' from its 'chain' at any time.
     * Last property deleted for a node is the next property added, but only after all nodes have been seen.
     * Outcome:
     * - All nodes have the same number of properties
     * - Number of properties on each node is stable throughout the experiment
     * - The set of properties between any two nodes may differ by at most two
     * - Each property is on (approx) same number of nodes --> every read does (approx) same amount of work
     */
    @Override
    public String description()
    {
        return "Tests impact on performance of having many unused indexes.\n" +
               "Specifically, when creating and deleting properties via Write::nodeRemoveProperty/nodeSetProperty.\n" +
               "Benchmark invariants:\n" +
               "- All nodes have the same number of properties\n" +
               "- Number of properties on each node is stable throughout the experiment\n" +
               "- The set of properties between any two nodes may differ by at most two\n" +
               "- Each property is on (almost exactly) same number of nodes --> every read does same amount of work";
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
                .withLabels( labels() )
                .withPropertyOrder( Order.ORDERED )
                .withNodeProperties( properties() )
                .withSchemaIndexes( indexes() )
                .isReusableStore( false )
                .build();
    }

    private Label[] labels()
    {
        switch ( label )
        {
        case NONE:
            return new Label[0];
        case INDEXED:
            return new Label[]{indexes()[0].label()}; // the first indexed label
        case UNINDEXED:
            return new Label[]{Label.label( "UnIndexed" )};
        default:
            throw new RuntimeException(
                    "Unrecognized label setting: " + label );
        }
    }

    private LabelKeyDefinition[] indexes()
    {
        return IntStream.range( 0, indexCount )
                        .mapToObj( i -> new LabelKeyDefinition(
                                Label.label(
                                        "Indexed" + (i / indexCountPerLabel) ),
                                UNUSED_KEY_PREFIX + (i % indexCountPerLabel) ) )
                        .toArray( LabelKeyDefinition[]::new );
    }

    private PropertyDefinition[] properties()
    {
        return IntStream.range( 0, propertyCount )
                        .mapToObj( i ->
                                           new PropertyDefinition(
                                                   type + "_" + i,
                                                   randPropertyFor( type ).value() ) )
                        .toArray( PropertyDefinition[]::new );
    }

    private String[] keys()
    {
        return Stream.of( properties() ).map( PropertyDefinition::key ).toArray( String[]::new );
    }

    @State( Scope.Thread )
    public static class WriteTxState extends AbstractKernelBenchmark.TxState
    {
        ValueGeneratorFun<Long> ids;
        ValueGeneratorFun values;
        int[] keys;

        int initialCreatePropertyId;
        int createPropertyId;
        int deletePropertyId;

        @Setup
        public void setUp(
                ThreadParams threadParams,
                CreateDeleteNodePropertiesWithUnusedIndexes benchmarkState,
                RNGState rngState ) throws KernelException
        {
            initializeTx( benchmarkState, benchmarkState.txSize );
            int threads = threadCountForSubgroupInstancesOf( threadParams );
            int thread = uniqueSubgroupThreadIdFor( threadParams );
            ids = nonContendingStridingFor(
                    LNG,
                    threads,
                    thread,
                    NODE_COUNT ).create();
            keys = propertyKeysToIds( benchmarkState.keys() );
            values =
                    randPropertyFor( benchmarkState.type ).value().create();
            // set to 'thread' so threads start at different offsets/labels
            initialCreatePropertyId = thread;
            createPropertyId = initialCreatePropertyId;
            updateProperties();
            advanceStoreToStableState( benchmarkState.db(), rngState.rng );
        }

        /**
         * Performs one pass of thread's node ID sequence, i.e., visits every node that it owns once.
         * At each node it visits it adds one property from 'properties[]' and removes the property next 'property[]'
         * index.
         * The property it add is already there, as nodes start with all 'properties[]' properties.
         * The property it removes is actually removed.
         * When the loop is complete the number of properties on each node in the store is equal to
         * properties[].length - 1,
         * which is the stable state.
         */
        private void advanceStoreToStableState( GraphDatabaseService db, SplittableRandom rng ) throws KernelException
        {
            do
            {
                kernelTx.advance();
                long nodeId = nodeId();
                kernelTx.write.nodeSetProperty( nodeId, createProperty(), value( rng ) );
                kernelTx.write.nodeRemoveProperty( nodeId, deleteProperty() );
                updateProperties();
            }
            while ( !ids.wrapped() );
            createPropertyId = ++initialCreatePropertyId;
            updateProperties();
        }

        long nodeId()
        {
            return ids.next( null );
        }

        int createProperty()
        {
            return keys[createPropertyId];
        }

        int deleteProperty()
        {
            return keys[deletePropertyId];
        }

        Value value( SplittableRandom rng )
        {
            return values.nextValue( rng );
        }

        public Write advance() throws InvalidTransactionTypeKernelException, TransactionFailureException
        {
            kernelTx.advance();
            if ( ids.wrapped() )
            {
                createPropertyId = ++initialCreatePropertyId;
                updateProperties();
            }
            else
            {
                updateProperties();
            }
            return kernelTx.write;
        }

        private void updateProperties()
        {
            createPropertyId = (createPropertyId + 1) % keys.length;
            deletePropertyId = (createPropertyId + 1) % keys.length;
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
    public Value createDeleteProperty( WriteTxState writeTxState, RNGState rngState )
            throws KernelException
    {
        Write write = writeTxState.advance();
        long nodeId = writeTxState.nodeId();
        write.nodeSetProperty( nodeId, writeTxState.createProperty(), writeTxState.value( rngState.rng ) );
        return write.nodeRemoveProperty( nodeId, writeTxState.deleteProperty() );
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateDeleteNodePropertiesWithUnusedIndexes.class, methods );
    }
}
