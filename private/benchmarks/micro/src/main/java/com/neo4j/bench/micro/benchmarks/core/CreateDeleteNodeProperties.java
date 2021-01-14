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
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.data.Augmenterizer;
import com.neo4j.bench.data.DataGenerator.Order;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.IndexType;
import com.neo4j.bench.data.LabelKeyDefinition;
import com.neo4j.bench.data.ManagedStore;
import com.neo4j.bench.data.PropertyDefinition;
import com.neo4j.bench.data.SplittableRandomProvider;
import com.neo4j.bench.data.Stores;
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
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import static com.neo4j.bench.data.DataGenerator.createSchemaIndex;
import static com.neo4j.bench.data.DataGenerator.waitForSchemaIndexes;
import static com.neo4j.bench.data.IndexType.COMPOSITE_SCHEMA;
import static com.neo4j.bench.data.IndexType.SCHEMA;
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
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateDeleteNodeProperties extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final int NODE_COUNT = 100_000;

    @ParamValues(
            allowed = {"NONE", "SCHEMA", "COMPOSITE_SCHEMA"},
            base = {"NONE", "SCHEMA", "COMPOSITE_SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @ParamValues(
            allowed = {"1", "10", "100", "1000"},
            base = {"1", "100"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {"4", "64"},
            base = {"4", "64"} )
    @Param( {} )
    public int count;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

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
        return "Tests performance of creating and deleting properties via " +
               "GraphDatabaseService::removeProperty/setProperty.\n" +
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
                .withLabels( LABEL )
                .withPropertyOrder( Order.ORDERED )
                .withNodeProperties( properties() )
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( record_format, format )
                                          .setTransactionMemory( txMemory )
                                          .build() )
                .isReusableStore( false )
                .build();
    }

    @Override
    protected Augmenterizer augmentDataGeneration()
    {
        return new Augmenterizer()
        {
            /**
             * Performs one pass of thread's node ID sequence, i.e., visits every node that it owns once.
             * At each node it visits it adds one property calculateFor 'properties[]' and removes the property next 'property[]' index.
             * The property it add is already there, as nodes start with all 'properties[]' properties.
             * The property it removes is actually removed.
             * When the loop is complete the number of properties on each node in the store is equal to properties[].length - 1,
             * which is the stable state.
             */
            @Override
            public void augment( int threads, Stores.StoreAndConfig storeAndConfig )
            {
                SplittableRandom rng = SplittableRandomProvider.newRandom( 0 );
                ValueGeneratorFun values = randPropertyFor( type ).value().create();
                GraphDatabaseService db = ManagedStore.newDb( storeAndConfig.store(), storeAndConfig.config() );
                List<String> keys = Stream.of( properties() ).map( PropertyDefinition::key ).collect( toList() );
                TxBatch txBatch = new TxBatch( db, 1000 );
                for ( int thread = 0; thread < threads; thread++ )
                {
                    int createPropertyId = incrementPropertyId( thread, keys.size() );
                    int deletePropertyId = incrementPropertyId( createPropertyId, keys.size() );
                    ValueGeneratorFun<Long> nodeIds = nonContendingStridingFor( LNG, threads, thread, NODE_COUNT ).create();
                    do
                    {
                        txBatch.advance();
                        Node node = txBatch.transaction().getNodeById( nodeIds.next( rng ) );
                        node.setProperty( keys.get( createPropertyId ), values.next( rng ) );
                        node.removeProperty( keys.get( deletePropertyId ) );
                        createPropertyId = incrementPropertyId( createPropertyId, keys.size() );
                        deletePropertyId = incrementPropertyId( createPropertyId, keys.size() );
                    }
                    while ( !nodeIds.wrapped() );
                }
                txBatch.close();

                if ( COMPOSITE_SCHEMA.equals( index ) || SCHEMA.equals( index ) )
                {
                    // Create indexes
                    LabelKeyDefinition[] indexes = indexes();
                    Stream.of( indexes ).forEach( def -> createSchemaIndex( db, def.label(), def.keys() ) );
                    waitForSchemaIndexes( db );
                }

                ManagedStore.getManagementService().shutdown();
            }
        };
    }

    private LabelKeyDefinition[] indexes()
    {
        switch ( index )
        {
        case NONE:
            return new LabelKeyDefinition[0];
        case SCHEMA:
            return Arrays.stream( keys() )
                         .map( key -> new LabelKeyDefinition( LABEL, key ) )
                         .toArray( LabelKeyDefinition[]::new );
        case COMPOSITE_SCHEMA:
            return compositeSchemaIndexDefinitions();
        default:
            throw new IllegalArgumentException( "Invalid index type: " + index );
        }
    }

    private LabelKeyDefinition[] compositeSchemaIndexDefinitions()
    {
        List<String> allKeys = Arrays.asList( keys() );
        LabelKeyDefinition[] labelKeyDefinitionsToIndex = new LabelKeyDefinition[allKeys.size()];
        int index = 0;
        for ( String keyToNotIncludeInIndex : allKeys )
        {
            List<String> keysToIndexList = copy( allKeys );
            keysToIndexList.remove( keyToNotIncludeInIndex );
            String[] keysToIndex = keysToIndexList.toArray( new String[0] );
            labelKeyDefinitionsToIndex[index++] = new LabelKeyDefinition( LABEL, keysToIndex );
        }
        return labelKeyDefinitionsToIndex;
    }

    private List<String> copy( List<String> list )
    {
        List<String> copy = new ArrayList<>( list.size() );
        copy.addAll( list );
        return copy;
    }

    private PropertyDefinition[] properties()
    {
        return IntStream.range( 0, count )
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

    private static int incrementPropertyId( int propertyId, int keyCount )
    {
        return (propertyId + 1) % keyCount;
    }

    @State( Scope.Thread )
    public static class WriteTxState
    {
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        ValueGeneratorFun values;
        String[] keys;

        int initialCreatePropertyId;
        int createPropertyId;
        int deletePropertyId;

        @SuppressWarnings( "unchecked" )
        @Setup
        public void setUp( ThreadParams threadParams, CreateDeleteNodeProperties benchmarkState )
        {
            int threads = threadCountForSubgroupInstancesOf( threadParams );
            int thread = uniqueSubgroupThreadIdFor( threadParams );
            ids = nonContendingStridingFor(
                    LNG,
                    threads,
                    thread,
                    NODE_COUNT ).create();
            keys = benchmarkState.keys();
            values = randPropertyFor( benchmarkState.type ).value().create();
            // set to 'thread' so threads start at different offsets/labels
            initialCreatePropertyId = thread;
            createPropertyId = initialCreatePropertyId;
            updateProperties();
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
            advanceStoreToStableState( benchmarkState.db() );
        }

        /**
         * Advance to first property key without a value (technically not even a property).
         * Every subsequent node in the node sequence for this thread will have the deleted property at the next higher keys offset, wrapping.
         */
        private void advanceStoreToStableState( GraphDatabaseService db )
        {
            txBatch.advance();
            Node node = txBatch.transaction().getNodeById( nodeId() );
            while ( node.hasProperty( deleteProperty() ) )
            {
                updateProperties();
            }
        }

        long nodeId()
        {
            return ids.next( null );
        }

        String createProperty()
        {
            return keys[createPropertyId];
        }

        String deleteProperty()
        {
            return keys[deletePropertyId];
        }

        Object value( SplittableRandom rng )
        {
            return values.next( rng );
        }

        void advance()
        {
            txBatch.advance();
            if ( ids.wrapped() )
            {
                createPropertyId = ++initialCreatePropertyId;
                updateProperties();
            }
            else
            {
                updateProperties();
            }
        }

        private void updateProperties()
        {
            createPropertyId = incrementPropertyId( createPropertyId, keys.length );
            deletePropertyId = incrementPropertyId( createPropertyId, keys.length );
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
    public void createDeleteProperty( WriteTxState writeTxState, RNGState rngState )
    {
        writeTxState.advance();
        Node node = writeTxState.txBatch.transaction().getNodeById( writeTxState.nodeId() );
        node.setProperty( writeTxState.createProperty(), writeTxState.value( rngState.rng ) );
        node.removeProperty( writeTxState.deleteProperty() );
    }
}
