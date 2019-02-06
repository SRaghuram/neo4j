/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

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
import com.neo4j.bench.micro.data.RelationshipDefinition;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.RelationshipDefinition.from;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class Expand extends AbstractCoreBenchmark
{
    public static final int NODE_COUNT = 10_000;
    public static final RelationshipDefinition[] RELATIONSHIP_DEFINITIONS =
            from( "(A:10),(B:10),(C:10),(D:10),(E:10),(F:10),(G:10),(H:10),(I:10),(J:10)" );

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

    @ParamValues(
            allowed = {"off_heap", "on_heap"},
            base = {"on_heap"} )
    @Param( {} )
    public String Expand_txMemory;

    @Override
    public String description()
    {
        return "Tests performance of expanding node using Core API.\n" +
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
                                          .withSetting( record_format, Expand_format )
                                          .setTransactionMemory( Expand_txMemory ) )
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;
        RelationshipType[] relationshipTypes;

        @Setup
        public void setUp( Expand benchmarkState ) throws InterruptedException
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

        public RelationshipType randomRelationshipType( SplittableRandom random )
        {
            int randomTypeIndex = random.nextInt( relationshipTypes.length );
            return relationshipTypes[randomTypeIndex];
        }
    }

    // Get Relationships

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void expandAll( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        Node node = db().getNodeById( nodeId );
        Iterable<Relationship> relationships = node.getRelationships( Direction.OUTGOING );
        for ( Relationship rel : relationships )
        {
            bh.consume( rel.getOtherNode( node ) );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void expandType( TxState txState, RNGState rngState, Blackhole bh )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        Node node = db().getNodeById( nodeId );
        RelationshipType type = txState.randomRelationshipType( rngState.rng );
        Iterable<Relationship> relationships = node.getRelationships( type, Direction.OUTGOING );
        for ( Relationship rel : relationships )
        {
            bh.consume( rel.getOtherNode( node ) );
        }
    }

    public static void main( String... methods ) throws Exception
    {
        run( Expand.class, methods );
    }
}
