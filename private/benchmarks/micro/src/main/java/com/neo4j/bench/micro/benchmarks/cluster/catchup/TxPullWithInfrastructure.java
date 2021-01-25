/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.cluster.TxFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class TxPullWithInfrastructure extends AbstractWithInfrastructureBenchmark
{
    private static final int TX_COUNT = 2;

    @ParamValues(
            allowed = {"V3", "V4", "LATEST"},
            base = {"V3", "V4", "LATEST"} )
    @Param( {} )
    public ProtocolVersion protocolVersion;

    @ParamValues(
            allowed = {"1KB", "1MB", "100MB", "1GB"},
            base = {"1KB", "1MB", "100MB", "1GB"} )
    @Param( {} )
    public String txSize;

    @Override
    public String description()
    {
        return "Pulling transactions with catchup protocol using catchup infrastructure";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    ProtocolVersion protocolVersion()
    {
        return protocolVersion;
    }

    @Override
    public void prepare() throws Throwable
    {
        catchupClientsWrapper.storeCopy();
        forceSnapshot();
        var txTotalSize = nbrOfBytes( txSize );
        for ( var i = 0; i < TX_COUNT; i++ )
        {
            TxFactory.commitOneNodeTx( txTotalSize / TX_COUNT, db() );
        }
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void pullTransactions() throws Exception
    {
        catchupClientsWrapper.pullTransactions();
    }

    public static void main( String... methods )
    {
        run( TxPullWithInfrastructure.class, methods );
    }
}
