/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.txlogs;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.DataGenerator.GraphWriter.TRANSACTIONAL;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;

public class WriteTransactionLogBenchmark extends AbstractTransactionLogsBenchmark
{
    @ParamValues( allowed = {"true", "false"}, base = {"true", "false"} )
    @Param( {} )
    private String preallocation;

    @ParamValues( allowed = {"10", "1000", "10000"}, base = {"10", "1000", "10000"} )
    @Param( {} )
    private int batch_size;

    @State( Scope.Thread )
    public static class ThreadState
    {
        private long[] longs;
        private byte[] bytes;

        @Setup
        public void setUp( WriteTransactionLogBenchmark benchmark, RNGState rngState ) throws InterruptedException
        {
            bytes = new byte[benchmark.batch_size];
            longs = rngState.rng.longs().limit( benchmark.batch_size ).toArray();
            rngState.rng.nextBytes(bytes);
        }
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder.empty()
                                                    .withSetting( preallocate_logical_logs, preallocation ).build();
        return new DataGeneratorConfigBuilder()
                .withGraphWriter( TRANSACTIONAL )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( false )
                .build();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( value = Mode.Throughput )
    public void appendLongs( ThreadState state ) throws IOException
    {
        for ( int i = 0; i < batch_size; i++ )
        {
            channel.putLong( state.longs[i] );
        }
        logFile.flush();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( value = Mode.Throughput )
    public void appendBytes( ThreadState state ) throws IOException
    {
        for ( int i = 0; i < batch_size; i++ )
        {
            channel.put( state.bytes[i] );
        }
        logFile.flush();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( value = Mode.Throughput )
    public void appendByteArray( ThreadState state ) throws IOException
    {
        channel.put( state.bytes, batch_size );
        logFile.flush();
    }

    @TearDown( Level.Invocation )
    public void flush() throws ExecutionException, InterruptedException, IOException
    {
        if ( logFile.rotationNeeded() )
        {
            logFile.rotate();
        }
    }

    @Override
    public String description()
    {
        return "Benchmarking transaction logs append benchmark";
    }

    public static void main( String... methods )
    {
        run( WriteTransactionLogBenchmark.class, methods );
    }
}
