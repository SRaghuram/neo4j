/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import org.neo4j.logging.LogProvider;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class TxPullBare extends AbstractBareBenchmark
{
    private static final int TX_COUNT = 2;
    private ClientSequence sequence;

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
        return "Pulling transactions with catchup protocol using only network components";
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

    void prepare( BareClient clientHandler ) throws InterruptedException
    {
        var txTotalSize = nbrOfBytes( txSize );
        transactionProvider.set( txTotalSize / TX_COUNT, TX_COUNT );
        new TxPullPrepareClientSequence( logProvider(), clientHandler ).perform( "foo" );
        sequence = new TxPullClientSequence( logProvider(), clientHandler );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void pullTransactions() throws Exception
    {
        sequence.perform( "foo" );
    }

    private static class TxPullPrepareClientSequence extends ClientSequence
    {
        TxPullPrepareClientSequence( LogProvider logProvider, BareClient clientHandler )
        {
            super( logProvider, clientHandler );
        }

        @Override
        protected void firstStep( String databaseName )
        {
            clientHandler.getDatabaseId( this::stepGetStoreId, databaseName );
        }

        private void stepGetStoreId()
        {
            clientHandler.getStoreId( this::stepPrepareStoreCopy );
        }

        private void stepPrepareStoreCopy()
        {
            clientHandler.prepareStoreCopy( this::finish );
        }
    }

    private static class TxPullClientSequence extends ClientSequence
    {
        TxPullClientSequence( LogProvider logProvider, BareClient clientHandler )
        {
            super( logProvider, clientHandler );
        }

        @Override
        protected void firstStep( String databaseName )
        {
            clientHandler.pullTransactions( this::finish );
        }
    }

    public static void main( String... methods )
    {
        run( TxPullBare.class, methods );
    }
}
