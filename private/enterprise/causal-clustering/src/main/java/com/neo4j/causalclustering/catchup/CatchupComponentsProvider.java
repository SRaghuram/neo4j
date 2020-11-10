/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.helpers.IncreasingTimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.causalclustering.net.BootstrapConfiguration.clientConfig;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static org.neo4j.monitoring.PanicEventGenerator.NO_OP;

/**
 * Contains a number of factories (or factories of factories) for machinery relating to the Catchup protocol.
 * This instance is typically Global.
 */
public final class CatchupComponentsProvider
{
    private static final String CATCHUP_SERVER_NAME = "catchup-server";
    private final PipelineBuilders pipelineBuilders;
    private final LogProvider logProvider;
    private final ApplicationSupportedProtocols supportedCatchupProtocols;
    private final List<ModifierSupportedProtocols> supportedModifierProtocols;
    private final Config config;
    private final LogProvider userLogProvider;
    private final JobScheduler scheduler;
    private final LifeSupport globalLife;
    private final CatchupClientFactory catchupClientFactory;
    private final ConnectorPortRegister portRegister;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystem;
    private final StorageEngineFactory storageEngineFactory;
    private final TimeoutStrategy storeCopyBackoffStrategy;
    private final DatabaseTracers databaseTracers;
    private final MemoryTracker otherMemoryGlobalTracker;
    private final SystemNanoClock clock;
    private final CompositeDatabaseAvailabilityGuard availabilityGuard;

    public CatchupComponentsProvider( GlobalModule globalModule, PipelineBuilders pipelineBuilders )
    {
        this.pipelineBuilders = pipelineBuilders;
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.config = globalModule.getGlobalConfig();
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        this.supportedCatchupProtocols = supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration();
        this.supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();
        this.userLogProvider = globalModule.getLogService().getUserLogProvider();
        this.scheduler = globalModule.getJobScheduler();
        this.pageCache = globalModule.getPageCache();
        this.globalLife = globalModule.getGlobalLife();
        this.fileSystem = globalModule.getFileSystem();
        this.storageEngineFactory = globalModule.getStorageEngineFactory();
        this.catchupClientFactory = createCatchupClientFactory();
        this.portRegister = globalModule.getConnectorPortRegister();
        this.databaseTracers = new DatabaseTracers( globalModule.getTracers() );
        this.otherMemoryGlobalTracker = globalModule.getOtherMemoryPool().getPoolMemoryTracker();
        this.clock = globalModule.getGlobalClock();
        this.copiedStoreRecovery = globalLife.add(
                new CopiedStoreRecovery( pageCache, databaseTracers, fileSystem, globalModule.getStorageEngineFactory(), otherMemoryGlobalTracker ) );
        final var maximumWait = config.get( CausalClusteringInternalSettings.store_copy_backoff_max_wait ).toMillis();
        this.storeCopyBackoffStrategy = new IncreasingTimeoutStrategy( 100, maximumWait, TimeUnit.MILLISECONDS,
                                                                                             i -> i + 100 );
        this.availabilityGuard = globalModule.getGlobalAvailabilityGuard();
    }

    private CatchupClientFactory createCatchupClientFactory()
    {
        CatchupClientFactory catchupClient = CatchupClientBuilder
                .builder()
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( pipelineBuilders.client() )
                .inactivityTimeout( config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ) )
                .scheduler( scheduler )
                .config( config )
                .bootstrapConfig( clientConfig( config ) )
                .commandReader( storageEngineFactory.commandReaderFactory() )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .debugLogProvider( logProvider )
                .clock( Clocks.nanoClock() )
                .build();
        globalLife.add( catchupClient );
        return catchupClient;
    }

    /**
     * Global Server instance for the Neo4j Catchup Protocol. Responds to store copy and catchup requests from other Neo4j instances
     * @return a catchup server
     */
    public Server createCatchupServer( InstalledProtocolHandler installedProtocolsHandler, CatchupServerHandler catchupServerHandler )
    {
        return CatchupServerBuilder.builder()
                .catchupServerHandler( catchupServerHandler )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( pipelineBuilders.server() )
                .installedProtocolsHandler( installedProtocolsHandler )
                .listenAddress( config.get( CausalClusteringSettings.transaction_listen_address ) )
                .scheduler( scheduler )
                .config( config )
                .bootstrapConfig( serverConfig( config ) )
                .portRegister( portRegister )
                .userLogProvider( userLogProvider )
                .debugLogProvider( logProvider )
                .serverName( CATCHUP_SERVER_NAME )
                .handshakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .build();
    }

    /**
     * Optional global server instance for the Neo4j Backup protocol. Basically works the same way as the catchup protocol.
     * @return an optional backup server
     */
    public Optional<Server> createBackupServer( InstalledProtocolHandler installedProtocolsHandler, CatchupServerHandler catchupServerHandler )
    {
        TransactionBackupServiceProvider transactionBackupServiceProvider =
                new TransactionBackupServiceProvider( logProvider, supportedCatchupProtocols,
                        supportedModifierProtocols,
                        pipelineBuilders.backupServer(),
                        catchupServerHandler,
                        installedProtocolsHandler,
                        scheduler,
                        portRegister );

        return transactionBackupServiceProvider.resolveIfBackupEnabled( config );
    }

    /**
     * Returns the per database machinery for initiating catchup and store copy requests
     * @param clusteredDatabaseContext the clustered database for which to produce catchup components
     * @return catchup protocol components for the provided clustered database
     */
    public CatchupComponentsRepository.CatchupComponents createDatabaseComponents( ClusteredDatabaseContext clusteredDatabaseContext )
    {
        var databaseLogProvider = clusteredDatabaseContext.database().getInternalLogProvider();
        var monitors = clusteredDatabaseContext.monitors();
        final var storeCopyExecutor = scheduler.executor( Group.STORE_COPY_CLIENT );
        var storeCopyClient = new StoreCopyClient( catchupClientFactory, clusteredDatabaseContext.databaseId(), () -> monitors, databaseLogProvider,
                                                   storeCopyExecutor, storeCopyBackoffStrategy, clock );
        var transactionLogFactory = new TransactionLogCatchUpFactory();
        var txPullClient = new TxPullClient( catchupClientFactory, clusteredDatabaseContext.databaseId(), () -> monitors, databaseLogProvider );

        var remoteStore = new RemoteStore( databaseLogProvider, fileSystem, pageCache, storeCopyClient, txPullClient, transactionLogFactory, config,
                                           monitors, storageEngineFactory, clusteredDatabaseContext.databaseId(), databaseTracers.getPageCacheTracer(),
                otherMemoryGlobalTracker, clock, availabilityGuard, new DatabaseHealth( NO_OP, logProvider.getLog( RemoteStore.class ) ) );

        var storeCopy = new StoreCopyProcess( fileSystem, pageCache, clusteredDatabaseContext, copiedStoreRecovery, remoteStore, databaseLogProvider );

        return new CatchupComponentsRepository.CatchupComponents( remoteStore, storeCopy );
    }

    public CatchupClientFactory catchupClientFactory()
    {
        return catchupClientFactory;
    }
}
