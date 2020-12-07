/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.cluster.TxFactory;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class StoreCopyWithInfrastructure extends AbstractWithInfrastructureBenchmark
{
    private static final int TX_COUNT = 16;

    @ParamValues(
            allowed = {"V3", "V4", "LATEST"},
            base = {"V3", "V4", "LATEST"} )
    @Param( {} )
    public ProtocolVersion protocolVersion;

    @ParamValues(
            allowed = {"64MB", "256MB", "1GB"},
            base = {"64MB", "256MB", "1GB"} )
    @Param( {} )
    public String prefilledDatabaseSize;

    @Override
    public String description()
    {
        return "Complete store copy with catchup protocol using catchup infrastructure";
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
    void prepare() throws Throwable
    {
        var txTotalSize = nbrOfBytes( prefilledDatabaseSize );
        for ( var i = 0; i < TX_COUNT; i++ )
        {
            TxFactory.commitOneNodeTx( txTotalSize / TX_COUNT, db() );
        }
        forceSnapshot();
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void copyStore() throws StoreIdDownloadFailedException, StoreCopyFailedException, CatchupAddressResolutionException
    {
        catchupClientsWrapper.storeCopy();
    }

    public static void main( String... methods )
    {
        run( StoreCopyWithInfrastructure.class, methods );
    }
}
