/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;
import com.neo4j.bench.micro.benchmarks.cluster.LocalNetworkPlatform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import java.util.function.Function;

import org.neo4j.io.ByteUnit;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class StoreCopyBare extends BaseRegularBenchmark
{
    private static boolean DEBUG;
    private static final int FILE_COUNT = 8;

    private Log log = logProvider().getLog( StoreCopyBare.class );

    private final LocalNetworkPlatform platform = new LocalNetworkPlatform();
    private final BareFilesHolder filesHolder = new BareFilesHolder();

    private Function<String,ClientSequence> startSequence;

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

    static LogProvider logProvider()
    {
        return DEBUG ? FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ) : NullLogProvider.getInstance();
    }

    @Override
    protected void benchmarkSetup( BenchmarkGroup group, com.neo4j.bench.model.model.Benchmark benchmark, Neo4jConfig neo4jConfig,
            ForkDirectory forkDirectory ) throws Throwable
    {
        log.info( "Starting preparing files" );
        var size = nbrOfBytes( filesSize );
        for ( var i = 0; i < FILE_COUNT; i++ )
        {
            filesHolder.prepareFile( "nonatomic" + i + ".bin", size / FILE_COUNT );
        }
        log.info( "Files prepared, preparing other stuff" );

        var logProvider = logProvider();
        var clientHandler = new BareClient( logProvider, platform );
        var serverHandler = new BareServer( filesHolder );
        var proto = new CatchupProtocolInstallers( logProvider, clientHandler, CommandReaderFactory.NO_COMMANDS, serverHandler );
        platform.start( proto, logProvider );
        startSequence = new StoreCopyClientSequence( logProvider, clientHandler )::start;

        log.info( "Other stuff prepared" );
    }

    @Override
    protected void benchmarkTearDown()
    {
        platform.stop();
        filesHolder.close();
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

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void copyStore() throws Exception
    {
        startSequence.apply( "foo" ).waitForFinish();
    }

    static int nbrOfBytes( String size )
    {
        return (int) ByteUnit.parse( size );
    }

    public static void main( String... methods )
    {
        run( StoreCopyBare.class, methods);
    }
}
