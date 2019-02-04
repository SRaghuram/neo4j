/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;

import java.util.Collection;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.net.BootstrapConfiguration.clientConfig;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;

public abstract class CatchupServersModule
{
    private static final String CATCHUP_SERVER_NAME = "catchup-server";

    protected Server catchupServer;
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    protected Optional<Server> backupServer;

    protected final CatchupClientFactory catchupClientFactory;
    protected final LogProvider logProvider;
    protected final Config globalConfig;
    protected final LifeSupport globalLife;
    protected final JobScheduler scheduler;

    private final ApplicationSupportedProtocols supportedCatchupProtocols;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final PipelineBuilders pipelineBuilders;
    private final CatchupComponentsService databaseComponents;
    private final LogProvider userLogProvider;
    private final ConnectorPortRegister portRegister;

    public CatchupServersModule( ClusteredDatabaseManager<? extends ClusteredDatabaseContext> clusteredDatabaseManager, PipelineBuilders pipelineBuilders,
            GlobalModule globalModule, String databaseName )
    {
        this.pipelineBuilders = pipelineBuilders;

        LogService logService = globalModule.getLogService();
        this.logProvider = logService.getInternalLogProvider();
        this.userLogProvider = logService.getUserLogProvider();
        this.globalConfig = globalModule.getGlobalConfig();
        this.globalLife = globalModule.getGlobalLife();
        this.scheduler = globalModule.getJobScheduler();
        this.portRegister = globalModule.getConnectorPortRegister();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( globalConfig, logProvider );
        this.supportedCatchupProtocols = supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration();
        this.supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        this.catchupClientFactory = createCatchupClient( databaseName );

        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        PageCache pageCache = globalModule.getPageCache();
        CopiedStoreRecovery copiedStoreRecovery = globalLife.add( new CopiedStoreRecovery( pageCache, fileSystem, globalModule.getStorageEngineFactory() ) );
        this.databaseComponents = new CatchupComponentsService( clusteredDatabaseManager, catchupClientFactory, copiedStoreRecovery, fileSystem,
                pageCache, globalConfig, logProvider, globalModule.getGlobalMonitors(), globalModule.getStorageEngineFactory() );
    }

    private CatchupClientFactory createCatchupClient( String databaseName )
    {
        CatchupClientFactory catchupClient = CatchupClientBuilder.builder()
                .defaultDatabaseName( databaseName )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( pipelineBuilders.client() )
                .inactivityTimeout( globalConfig.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ) )
                .scheduler( scheduler )
                .bootstrapConfig( clientConfig( globalConfig ) )
                .handShakeTimeout( globalConfig.get( CausalClusteringSettings.handshake_timeout ) )
                .debugLogProvider( logProvider )
                .userLogProvider( userLogProvider )
                .build();
        globalLife.add( catchupClient );
        return catchupClient;
    }

    protected final Server createCatchupServer( InstalledProtocolHandler installedProtocolsHandler, CatchupServerHandler catchupServerHandler,
            String databaseName )
    {
        return CatchupServerBuilder.builder()
                .catchupServerHandler( catchupServerHandler )
                .defaultDatabaseName( databaseName )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( pipelineBuilders.server() )
                .installedProtocolsHandler( installedProtocolsHandler )
                .listenAddress( globalConfig.get( CausalClusteringSettings.transaction_listen_address ) )
                .scheduler( scheduler )
                .bootstrapConfig( serverConfig( globalConfig ) )
                .userLogProvider( userLogProvider )
                .debugLogProvider( logProvider )
                .serverName( CATCHUP_SERVER_NAME )
                .build();
    }

    protected final Optional<Server> createBackupServer( InstalledProtocolHandler installedProtocolsHandler, CatchupServerHandler catchupServerHandler,
            String databaseName )
    {
        TransactionBackupServiceProvider transactionBackupServiceProvider =
                new TransactionBackupServiceProvider( logProvider, supportedCatchupProtocols,
                        supportedModifierProtocols,
                        pipelineBuilders.backupServer(),
                        catchupServerHandler,
                        installedProtocolsHandler,
                        scheduler,
                        portRegister );

        return transactionBackupServiceProvider.resolveIfBackupEnabled( globalConfig, databaseName );
    }

    public final CatchupClientFactory catchupClient()
    {
        return catchupClientFactory;
    }

    public final CatchupComponentsRepository catchupComponents()
    {
        return databaseComponents;
    }

    public abstract Server catchupServer();

    public abstract Optional<Server> backupServer();
}
