/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
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

import java.util.SplittableRandom;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.Degrees;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.Expand.NODE_COUNT;
import static com.neo4j.bench.micro.benchmarks.core.Expand.RELATIONSHIP_DEFINITIONS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class Expand extends AbstractKernelBenchmark
{
    @ParamValues(
            allowed = {"SCATTERED_BY_START_NODE", "CO_LOCATED_BY_START_NODE"},
            base = {"SCATTERED_BY_START_NODE"} )
    @Param( {} )
    public RelationshipLocality locality;

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
    public String description()
    {
        return "Tests performance of expanding nodes using Kernel API.\n" +
               "Method:\n" +
               "- Every node has the same number of relationships\n" +
               "- Every relationship type appears with the same frequency\n" +
               "- Relationship chains are shuffled, i.e., are added in random order, regardless of type\n" +
               "- When reading, node IDs are selected using uniform random policy";
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
                .withOutRelationships( RELATIONSHIP_DEFINITIONS )
                .withRelationshipLocality( locality )
                .withRelationshipOrder( Order.SHUFFLED )
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .setDense( dense )
                                          .withSetting( record_format, format )
                                          .build() )
                .isReusableStore( true )
                .build();
    }

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        NodeCursor node;
        RelationshipTraversalCursor edge;
        int[] relationshipTypeIds;

        @Setup
        public void setUp( Expand benchmark ) throws Exception
        {
            initializeTx( benchmark );
            node = kernelTx.cursors.allocateNodeCursor( NULL );
            edge = kernelTx.cursors.allocateRelationshipTraversalCursor( NULL );

            relationshipTypeIds = Stream.of( RELATIONSHIP_DEFINITIONS )
                                        .mapToInt( this::relationshipTypeToId )
                                        .toArray();
        }

        @TearDown
        public void tearDown() throws Exception
        {
            node.close();
            edge.close();
            closeTx();
        }

        int randomRelationshipType( SplittableRandom random )
        {
            int randomTypeIndex = random.nextInt( relationshipTypeIds.length );
            return relationshipTypeIds[randomTypeIndex];
        }
    }

    // Traverse

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void expandAllDirect( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        txState.node.next();
        txState.node.relationships( txState.edge, ALL_RELATIONSHIPS );

        while ( txState.edge.next() )
        {
            txState.edge.otherNode( txState.node );
            txState.node.next();
            bh.consume( txState.node.propertiesReference() );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void expandTypeDirect( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        int type = txState.randomRelationshipType( rngState.rng );

        txState.node.next();
        txState.node.relationships( txState.edge, selection( type, Direction.BOTH ) );

        while ( txState.edge.next() )
        {
            txState.edge.otherNode( txState.node );
            txState.node.next();
            bh.consume( txState.node.propertiesReference() );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void degreesSingleType( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        int type = txState.randomRelationshipType( rngState.rng );

        txState.node.next();
        Degrees degrees = txState.node.degrees( selection( type, Direction.BOTH ) );

        bh.consume( degrees.totalDegree( type ) );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void allDegrees( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        txState.node.next();
        Degrees degrees = txState.node.degrees( ALL_RELATIONSHIPS );

        bh.consume( degrees.totalDegree() );
    }

    public static void main( String... methods ) throws Exception
    {
        run( Expand.class, methods );
    }
}
