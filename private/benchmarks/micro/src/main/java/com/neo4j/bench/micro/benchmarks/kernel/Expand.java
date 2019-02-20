/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

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

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.Expand.NODE_COUNT;
import static com.neo4j.bench.micro.benchmarks.core.Expand.RELATIONSHIP_DEFINITIONS;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class Expand extends AbstractKernelBenchmark
{
    @ParamValues(
            allowed = {"SCATTERED_BY_START_NODE", "CO_LOCATED_BY_START_NODE"},
            base = {"SCATTERED_BY_START_NODE"} )
    @Param( {} )
    public RelationshipLocality Expand_locality;

    @ParamValues(
            allowed = {"true", "false"},
            base = {"true"} )
    @Param( {} )
    public boolean Expand_dense;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String Expand_format;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation Expand_kernelImplementation;

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
                .withRelationshipLocality( Expand_locality )
                .withRelationshipOrder( Order.SHUFFLED )
                .withNeo4jConfig( Neo4jConfig
                                          .empty()
                                          .setDense( Expand_dense )
                                          .withSetting( record_format, Expand_format ) )
                .isReusableStore( true )
                .build();
    }

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return Expand_kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        NodeCursor node;
        RelationshipTraversalCursor edge;
        RelationshipGroupCursor edgeGroup;
        int[] relationshipTypeIds;

        @Setup
        public void setUp( Expand benchmark ) throws Exception
        {
            initializeTx( benchmark );
            node = kernelTx.cursors.allocateNodeCursor();
            edge = kernelTx.cursors.allocateRelationshipTraversalCursor();
            edgeGroup = kernelTx.cursors.allocateRelationshipGroupCursor();

            relationshipTypeIds = Stream.of( RELATIONSHIP_DEFINITIONS )
                                        .mapToInt( this::relationshipTypeToId )
                                        .toArray();
        }

        @TearDown
        public void tearDown() throws Exception
        {
            node.close();
            edge.close();
            edgeGroup.close();
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
    public void expandAllByGroup( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        txState.node.next();
        txState.node.relationships( txState.edgeGroup );

        while ( txState.edgeGroup.next() )
        {
            txState.edgeGroup.outgoing( txState.edge );
            while ( txState.edge.next() )
            {
                txState.edge.neighbour( txState.node );
                txState.node.next();
                bh.consume( txState.node.propertiesReference() );
            }
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void expandTypeByGroup( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        int type = txState.randomRelationshipType( rngState.rng );

        txState.node.next();
        txState.node.relationships( txState.edgeGroup );

        while ( txState.edgeGroup.next() )
        {
            if ( txState.edgeGroup.type() == type )
            {
                txState.edgeGroup.outgoing( txState.edge );
                while ( txState.edge.next() )
                {
                    txState.edge.neighbour( txState.node );
                    txState.node.next();
                    bh.consume( txState.node.propertiesReference() );
                }
                return;
            }
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void expandAllDirect( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.kernelTx.read.singleNode( nodeId, txState.node );

        txState.node.next();
        txState.node.allRelationships( txState.edge );

        while ( txState.edge.next() )
        {
            txState.edge.neighbour( txState.node );
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
        txState.node.allRelationships( txState.edge );

        while ( txState.edge.next() )
        {
            if ( txState.edge.type() == type )
            {
                txState.edge.neighbour( txState.node );
                txState.node.next();
                bh.consume( txState.node.propertiesReference() );
            }
        }
    }

    public static void main( String... methods ) throws Exception
    {
        run( Expand.class, methods );
    }
}
