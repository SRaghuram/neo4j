/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.RelationshipDefinition;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;

import static com.neo4j.bench.micro.data.RelationshipDefinition.from;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class ReadRelationship extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 10_000;
    private static final RelationshipDefinition[] RELATIONSHIP_DEFINITIONS =
            from( "(A:10),(B:10),(C:10),(D:10),(E:10),(F:10),(G:10),(H:10),(I:10),(J:10)" );

    @ParamValues(
            allowed = {"SCATTERED_BY_START_NODE", "CO_LOCATED_BY_START_NODE"},
            base = {"SCATTERED_BY_START_NODE"} )
    @Param( {} )
    public RelationshipLocality ReadRelationship_locality;

    @ParamValues(
            allowed = {"true", "false"},
            base = {"true"} )
    @Param( {} )
    public boolean ReadRelationship_dense;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String ReadRelationship_format;

    @ParamValues(
            allowed = {"off_heap", "on_heap"},
            base = {"on_heap"} )
    @Param( {} )
    public String ReadRelationship_txMemory;

    @Override
    public String description()
    {
        return "Tests performance of retrieving relationships.\n" +
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
        Neo4jConfig neo4jConfig = Neo4jConfig
                .empty()
                .withSetting( dense_node_threshold, denseNodeThreshold() )
                .withSetting( record_format, ReadRelationship_format )
                .setTransactionMemory( ReadRelationship_txMemory );
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( RELATIONSHIP_DEFINITIONS )
                .withRelationshipLocality( ReadRelationship_locality )
                .withRelationshipOrder( Order.SHUFFLED )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( true )
                .build();
    }

    private String denseNodeThreshold()
    {
        return ReadRelationship_dense
               // dense node threshold set to min --> all nodes are dense
               ? "1"
               // dense node threshold set to max --> no nodes are dense
               : Integer.toString( Integer.MAX_VALUE );
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;
        RelationshipType[] relationshipTypes;

        @Setup
        public void setUp( ReadRelationship benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
            relationshipTypes = Stream.of( RELATIONSHIP_DEFINITIONS )
                    .map( RelationshipDefinition::type )
                    .toArray( RelationshipType[]::new );
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    // Get Relationships

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long countRelationships( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return Iterables.count( db().getNodeById( nodeId ).getRelationships( Direction.BOTH ) );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long countRelationshipsDirected( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return Iterables.count( db().getNodeById( nodeId ).getRelationships( Direction.OUTGOING ) );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long countRelationshipsTyped( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomTypeIndex = rngState.rng.nextInt( txState.relationshipTypes.length );
        RelationshipType type = txState.relationshipTypes[randomTypeIndex];
        return Iterables.count( db().getNodeById( nodeId ).getRelationships( type, Direction.BOTH ) );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long countRelationshipsDirectedTyped( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomTypeIndex = rngState.rng.nextInt( txState.relationshipTypes.length );
        RelationshipType type = txState.relationshipTypes[randomTypeIndex];
        return Iterables.count( db().getNodeById( nodeId ).getRelationships( type, Direction.OUTGOING ) );
    }

    // Has Relationship

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasRelationship( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return db().getNodeById( nodeId ).hasRelationship( Direction.BOTH );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasRelationshipDirected( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return db().getNodeById( nodeId ).hasRelationship( Direction.OUTGOING );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasRelationshipTyped( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomTypeIndex = rngState.rng.nextInt( txState.relationshipTypes.length );
        RelationshipType type = txState.relationshipTypes[randomTypeIndex];
        return db().getNodeById( nodeId ).hasRelationship( type, Direction.BOTH );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasRelationshipDirectedTyped( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomTypeIndex = rngState.rng.nextInt( txState.relationshipTypes.length );
        RelationshipType type = txState.relationshipTypes[randomTypeIndex];
        return db().getNodeById( nodeId ).hasRelationship( type, Direction.OUTGOING );
    }

    // Get Degree

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public int getDegree( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return db().getNodeById( nodeId ).getDegree( Direction.BOTH );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public int getDegreeDirected( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        return db().getNodeById( nodeId ).getDegree( Direction.OUTGOING );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public int getDegreeTyped( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomTypeIndex = rngState.rng.nextInt( txState.relationshipTypes.length );
        RelationshipType type = txState.relationshipTypes[randomTypeIndex];
        return db().getNodeById( nodeId ).getDegree( type, Direction.BOTH );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public int getDegreeDirectedTyped( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomTypeIndex = rngState.rng.nextInt( txState.relationshipTypes.length );
        RelationshipType type = txState.relationshipTypes[randomTypeIndex];
        return db().getNodeById( nodeId ).getDegree( type, Direction.OUTGOING );
    }
}
