/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.PropertyDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.ReadNodeProperty.NODE_COUNT;
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
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_INL;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.randPropertyFor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReadNodeProperty extends AbstractKernelBenchmark
{
    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_INL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_INL, STR_SML, POINT, DATE_TIME} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"standard"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @Override
    public String description()
    {
        return "Tests performance of retrieving properties from nodes that have a single property.\n" +
               "Method:\n" +
               "- Every node has the same property (with different values)\n" +
               "- During store creation, property values are generated with uniform random policy\n" +
               "- When reading, node IDs are selected using two different policies: same, random\n" +
               "--- same: Same node accessed every time. Best cache hit rate. Test cached performance.\n" +
               "--- random: Random node accessed every time. Worst cache hit rate. Test non-cached performance.\n" +
               "Outcome:\n" +
               "- Tests performance of property reading in cached & non-cached scenarios";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        PropertyDefinition propertyDefinition = randPropertyFor( type );
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withNodeProperties( propertyDefinition )
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
        PropertyCursor property;
        Read read;
        int propertyKeyId;

        @Setup
        public void setUp( ReadNodeProperty benchmark ) throws Exception
        {
            initializeTx( benchmark );
            node = kernelTx.cursors.allocateNodeCursor( NULL );
            property = kernelTx.cursors.allocatePropertyCursor( NULL, INSTANCE );
            read = kernelTx.read;
            propertyKeyId = propertyKeyToId( randPropertyFor( benchmark.type ) );
        }

        @TearDown
        public void tearDown() throws Exception
        {
            node.close();
            property.close();
            closeTx();
        }
    }

    // --------------- SAME NODE ---------------

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public Object sameNodeGetProperty( TxState txState )
    {
        txState.read.singleNode( 1, txState.node );
        txState.node.next();
        txState.node.properties( txState.property );
        while ( txState.property.next() )
        {
            if ( txState.property.propertyKey() == txState.propertyKeyId )
            {
                return txState.property.propertyValue();
            }
        }
        throw new Kaboom();
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean sameNodeHasProperty( TxState txState )
    {
        txState.read.singleNode( 1, txState.node );
        txState.node.next();
        txState.node.properties( txState.property );
        while ( txState.property.next() )
        {
            if ( txState.property.propertyKey() == txState.propertyKeyId )
            {
                return true;
            }
        }
        throw new Kaboom();
    }

    // --------------- RANDOM NODE ---------------

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public Object randomNodeGetProperty( TxState txState, RNGState rngState )
    {
        txState.read.singleNode( rngState.rng.nextInt( NODE_COUNT ), txState.node );
        txState.node.next();
        txState.node.properties( txState.property );
        while ( txState.property.next() )
        {
            if ( txState.property.propertyKey() == txState.propertyKeyId )
            {
                return txState.property.propertyValue();
            }
        }
        throw new Kaboom();
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean randomNodeHasProperty( TxState txState, RNGState rngState )
    {
        txState.read.singleNode( rngState.rng.nextInt( NODE_COUNT ), txState.node );
        txState.node.next();
        txState.node.properties( txState.property );
        while ( txState.property.next() )
        {
            if ( txState.property.propertyKey() == txState.propertyKeyId )
            {
                return true;
            }
        }
        throw new Kaboom();
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReadNodeProperty.class, methods );
    }
}
