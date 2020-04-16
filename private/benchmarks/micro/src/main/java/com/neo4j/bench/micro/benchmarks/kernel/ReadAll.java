/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.schema.IndexOrder;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.ReadAll.LABEL;
import static com.neo4j.bench.micro.benchmarks.core.ReadAll.NODE_COUNT;
import static com.neo4j.bench.micro.benchmarks.core.ReadAll.RELATIONSHIP_COUNT;
import static com.neo4j.bench.micro.benchmarks.core.ReadAll.RELATIONSHIP_DEFINITION;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.openjdk.jmh.annotations.Mode.SampleTime;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReadAll extends AbstractKernelBenchmark
{
    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @Override
    public String description()
    {
        return "Tests performance of iterating through all nodes/relationships using Kernel API.";
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
                .withOutRelationships( RELATIONSHIP_DEFINITION )
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
        NodeLabelIndexCursor nodeByLabel;
        RelationshipScanCursor edge;
        Read read;
        int labelId;

        @Setup
        public void setUp( ReadAll benchmark ) throws Exception
        {
            initializeTx( benchmark );
            node = kernelTx.cursors.allocateNodeCursor( NULL );
            nodeByLabel = kernelTx.cursors.allocateNodeLabelIndexCursor( NULL );
            edge = kernelTx.cursors.allocateRelationshipScanCursor( NULL );
            read = kernelTx.read;
            labelId = labelToId( LABEL );
        }

        @TearDown
        public void tearDown() throws Exception
        {
            node.close();
            edge.close();
            closeTx();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabel( TxState txState, Blackhole bh )
    {
        NodeLabelIndexCursor node = txState.nodeByLabel;
        txState.read.nodeLabelScan( txState.labelId, node, IndexOrder.NONE );
        assertCount( node, NODE_COUNT, bh );
    }

    @Benchmark
    @BenchmarkMode( SampleTime )
    public void allNodes( TxState txState, Blackhole bh )
    {
        NodeCursor node = txState.node;
        txState.read.allNodesScan( node );
        assertCount( node, NODE_COUNT, bh );
    }

    @Benchmark
    @BenchmarkMode( SampleTime )
    public void allRelationships( TxState txState, Blackhole bh )
    {
        RelationshipScanCursor edge = txState.edge;
        txState.read.allRelationshipsScan( edge );
        assertCount( edge, RELATIONSHIP_COUNT, bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReadAll.class, methods );
    }
}
