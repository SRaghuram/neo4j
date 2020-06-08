/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.cluster.EditionModuleBackedAbstractBenchmark;
import com.neo4j.bench.micro.benchmarks.cluster.TxFactory;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import java.time.Duration;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class StoreCopyWithInfrastructure extends EditionModuleBackedAbstractBenchmark
{
    private static boolean DEBUG;
    private static final int TX_COUNT = 16;

    private Log log = logProvider().getLog( getClass() );
    private SocketAddress socketAddress = new SocketAddress( "localhost", 46870 );
    private Server catchupServer;
    private CatchupClientFactory catchupClientFactory;
    private StoreCopyClientWrapper storeCopyClient;

    @ParamValues(
            allowed = {"64MB", "256MB", "1GB"},
            base = {"64MB", "256MB", "1GB"} )
    @Param( {} )
    public String prefilledDatabaseSize;

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
    protected AbstractEditionModule createModule( GlobalModule globalModule )
    {
        return new CommunityEditionModule( globalModule );
    }

    @Override
    public void setUp() throws Throwable
    {
        log.info( "Prepare start" );
        var txTotalSize = nbrOfBytes( prefilledDatabaseSize );
        for ( var i = 0; i < TX_COUNT; i++ )
        {
            TxFactory.commitOneNodeTx( txTotalSize / TX_COUNT, db() );
        }
        db().getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile( LogAppendEvent.NULL );
        db().getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "measure" ) );
        catchupServer = createCatchupServer();
        catchupServer.start();
        catchupClientFactory = createCatchupClientFactory();
        catchupClientFactory.start();
        storeCopyClient = new StoreCopyClientWrapper( module(), catchupClientFactory, db().databaseId(), logProvider(), socketAddress );
        log.info( "Prepare done" );
    }

    @Override
    public void shutdown()
    {
        catchupServer.stop();
        catchupClientFactory.stop();
    }

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

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void copyStore() throws StoreIdDownloadFailedException, StoreCopyFailedException, CatchupAddressResolutionException
    {
        storeCopyClient.storeCopy();
    }

    private CatchupClientFactory createCatchupClientFactory()
    {
        var logProvider = logProvider();
        var supportedProtocolCreator = new SupportedProtocolCreator( config(), logProvider );
        var dependencyResolver = db().getDependencyResolver();
        return CatchupClientBuilder.builder()
                .catchupProtocols( supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration() )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .inactivityTimeout( Duration.ofSeconds( 5 ) )
                .scheduler( dependencyResolver.resolveDependency( JobScheduler.class ) )
                .bootstrapConfig( BootstrapConfiguration.clientConfig( config() ) )
                .commandReader( module().getStorageEngineFactory().commandReaderFactory() )
                .build();
    }

    private Server createCatchupServer()
    {
        var logProvider = logProvider();
        var supportedProtocolCreator = new SupportedProtocolCreator( config(), logProvider );
        var dependencyResolver = db().getDependencyResolver();
        return CatchupServerBuilder.builder()
                .catchupServerHandler( new MultiDatabaseCatchupServerHandler( dependencyResolver.resolveDependency( DatabaseManager.class ),
                        dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                        config().get( CausalClusteringSettings.store_copy_chunk_size ),
                        logProvider ) )
                .catchupProtocols( supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration() )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .installedProtocolsHandler( new InstalledProtocolHandler() )
                .listenAddress( socketAddress )
                .scheduler( dependencyResolver.resolveDependency( JobScheduler.class ) )
                .config( config() )
                .bootstrapConfig( BootstrapConfiguration.serverConfig( config() ) )
                .portRegister( module().getConnectorPortRegister() )
                .build();
    }

    static int nbrOfBytes( String size )
    {
        return (int) ByteUnit.parse( size );
    }

    public static void main( String... methods )
    {
        run( StoreCopyWithInfrastructure.class, methods );
    }
}
