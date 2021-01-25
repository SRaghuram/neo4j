/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;
import com.neo4j.bench.micro.benchmarks.cluster.LocalNetworkPlatform;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;

import org.neo4j.io.ByteUnit;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;

abstract class AbstractBareBenchmark extends BaseRegularBenchmark
{
    protected static boolean DEBUG;
    protected Log log = logProvider().getLog( StoreCopyBare.class );

    private final LocalNetworkPlatform platform = new LocalNetworkPlatform();
    protected final BareFilesHolder filesHolder = new BareFilesHolder();
    protected final BareTransactionProvider transactionProvider = new BareTransactionProvider();

    static LogProvider logProvider()
    {
        return DEBUG ? new Log4jLogProvider( System.out, Level.DEBUG ) : NullLogProvider.getInstance();
    }

    static int nbrOfBytes( String size )
    {
        return (int) ByteUnit.parse( size );
    }

    @Override
    public String benchmarkGroup()
    {
        return "Catchup";
    }

    @Override
    protected void benchmarkSetup( BenchmarkGroup group,
            Benchmark benchmark,
            Neo4jConfig neo4jConfig,
            ForkDirectory forkDirectory ) throws Throwable
    {
        var logProvider = logProvider();
        log.info( "Prepare start" );
        var clientHandler = new BareClient( logProvider, platform );
        var serverHandler = new BareServer( logProvider, filesHolder, transactionProvider );
        var proto = new CatchupProtocolInstallers( protocolVersion(), logProvider, clientHandler, BareTransactionProvider.COMMAND_FACTORY, serverHandler );
        platform.start( proto, logProvider );
        prepare( clientHandler );
        log.info( "Prepare done" );
    }

    @Override
    protected void benchmarkTearDown()
    {
        platform.stop();
        filesHolder.close();
        transactionProvider.set( 0, 0 );
    }

    abstract ProtocolVersion protocolVersion();

    abstract void prepare( BareClient clientHandler ) throws Throwable;
}
