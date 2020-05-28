/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.micro.benchmarks.cluster.EditionModuleBackedAbstractBenchmark;
import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.configuration.CausalClusteringSettings;

import java.io.IOException;
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

abstract class AbstractWithInfrastructureBenchmark extends EditionModuleBackedAbstractBenchmark
{
    protected static boolean DEBUG;
    protected Log log = logProvider().getLog( getClass() );

    private CatchupClientFactory catchupClientFactory;
    private Server catchupServer;

    protected CatchupClientsWrapper catchupClientsWrapper;

    static LogProvider logProvider()
    {
        return DEBUG ? FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ) : NullLogProvider.getInstance();
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
    protected AbstractEditionModule createModule( GlobalModule globalModule )
    {
        return new CommunityEditionModule( globalModule );
    }

    @Override
    public void setUp() throws Throwable
    {
        var socketAddress = new SocketAddress( "localhost", 46871 );
        catchupServer = createCatchupServer( socketAddress );
        catchupServer.start();
        catchupClientFactory = createCatchupClientFactory();
        catchupClientFactory.start();
        catchupClientsWrapper = new CatchupClientsWrapper( module(), catchupClientFactory, db().databaseId(), logProvider(), socketAddress );
        prepare();
    }

    abstract void prepare() throws Throwable;

    @Override
    public void shutdown()
    {
        catchupServer.stop();
        catchupClientFactory.stop();
    }

    private CatchupClientFactory createCatchupClientFactory()
    {
        var logProvider = AbstractWithInfrastructureBenchmark.logProvider();
        var supportedProtocolCreator = new SupportedProtocolCreator( config(), logProvider );
        var dependencyResolver = db().getDependencyResolver();
        return CatchupClientBuilder.builder()
                .catchupProtocols( supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration() )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .inactivityTimeout( Duration.ofSeconds( 60 ) )
                .scheduler( dependencyResolver.resolveDependency( JobScheduler.class ) )
                .bootstrapConfig( BootstrapConfiguration.clientConfig( config() ) )
                .commandReader( module().getStorageEngineFactory().commandReaderFactory() )
                .build();
    }

    private Server createCatchupServer( SocketAddress socketAddress )
    {
        var logProvider = AbstractWithInfrastructureBenchmark.logProvider();
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

    void forceSnapshot() throws IOException
    {
        db().getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile( LogAppendEvent.NULL );
        db().getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "measure" ) );
    }
}
