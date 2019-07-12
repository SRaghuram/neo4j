/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.test;

import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.bench.micro.data.Augmenterizer;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ManagedStore;
import com.neo4j.bench.micro.data.RelationshipDefinition;
import com.neo4j.bench.micro.data.Stores;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

public class ConstantDataConstantAugment extends BaseDatabaseBenchmark
{
    private static final int NODES = 2;
    private static final int OUT_RELATIONSHIPS = 1;

    @ParamValues(
            allowed = {"1", "2"},
            base = {"1", "2"} )
    @Param( {} )
    public int ConstantDataConstantAugment_extraNodes;

    @Override
    public String description()
    {
        return "Constant Augmentation";
    }

    @Override
    public String benchmarkGroup()
    {
        return "TestOnly";
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
                .withNodeCount( NODES )
                .withOutRelationships( new RelationshipDefinition( withName( "TEST_TYPE" ), OUT_RELATIONSHIPS ) )
                .withLabels( label( "TestLabel" ) )
                .isReusableStore( true )
                .build();
    }

    @Override
    protected Augmenterizer augmentDataGeneration()
    {
        return new Augmenterizer()
        {
            @Override
            public void augment( int threads, Stores.StoreAndConfig storeAndConfig )
            {
                GraphDatabaseService db = ManagedStore.newDb( storeAndConfig.store(), storeAndConfig.config() );
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode();
                    tx.success();
                }
                ManagedStore.getManagementService().shutdown();
            }

            @Override
            public String augmentKey( FullBenchmarkName benchmarkName )
            {
                return "constant";
            }
        };
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Transaction tx;

        @Setup
        public void setUp( ConstantDataConstantAugment benchmarkState )
        {
            tx = benchmarkState.db().beginTx();
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public long method1( TxState txState )
    {
        return countThings();
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public long method2( TxState txState )
    {
        return countThings();
    }

    private long countThings()
    {
        long nodeCount = 0;
        long relationshipCount = 0;
        for ( Node n : db().getAllNodes() )
        {
            nodeCount++;
            for ( Relationship r : n.getRelationships( OUTGOING ) )
            {
                relationshipCount++;
            }
        }
        int expectedNodes = NODES + 1;
        if ( nodeCount != expectedNodes )
        {
            throw new Kaboom( "Expected " + expectedNodes + " nodes but found: " + nodeCount );
        }
        int expectedRelationships = NODES * OUT_RELATIONSHIPS;
        if ( relationshipCount != expectedRelationships )
        {
            throw new Kaboom( "Expected " + expectedRelationships + " relationships but found: " + relationshipCount );
        }
        return nodeCount + relationshipCount;
    }
}
