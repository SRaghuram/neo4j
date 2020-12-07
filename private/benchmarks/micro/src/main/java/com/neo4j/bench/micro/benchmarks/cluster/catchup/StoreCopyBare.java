/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
public class StoreCopyBare extends AbstractBareBenchmark
{
    private static final int FILE_COUNT = 8;
    private ClientSequence sequence;

    @ParamValues(
            allowed = {"V3", "V4", "LATEST"},
            base = {"V3", "V4", "LATEST"} )
    @Param( {} )
    public ProtocolVersion protocolVersion;

    @ParamValues(
            allowed = {"64MB", "256MB", "1GB"},
            base = {"64MB", "256MB", "1GB"} )
    @Param( {} )
    public String filesSize;

    @Override
    public String benchmarkGroup()
    {
        return "Catchup";
    }

    @Override
    void prepare( BareClient clientHandler ) throws Throwable
    {
        log.info( "Starting preparing files" );
        var size = nbrOfBytes( filesSize );
        for ( var i = 0; i < FILE_COUNT; i++ )
        {
            filesHolder.prepareFile( "nonatomic" + i + ".bin", size / FILE_COUNT );
        }
        log.info( "Files prepared, preparing other stuff" );
        sequence = new StoreCopyClientSequence( logProvider(), clientHandler );
    }

    @Override
    public String description()
    {
        return "Complete store copy with catchup protocol using only network components";
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

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void copyStore() throws Exception
    {
        sequence.perform( "foo" );
    }

    private static class StoreCopyClientSequence extends ClientSequence
    {

        StoreCopyClientSequence( LogProvider logProvider, BareClient clientHandler )
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
            clientHandler.prepareStoreCopy( this::stepGetFile );
        }

        private void stepGetFile()
        {
            if ( clientHandler.hasNextFile() )
            {
                clientHandler.getNextFile( this::stepGetFile );
            }
            else
            {
                clientHandler.pullTransactions( this::finish );
            }
        }
    }

    public static void main( String... methods )
    {
        run( StoreCopyBare.class, methods);
    }
}
