/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import com.neo4j.causalclustering.common.DatabaseService;
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
import org.neo4j.configuration.ConnectorPortRegister;
import org.neo4j.configuration.GraphDatabaseSettings;
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
    protected Server catchupServer;
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    protected Optional<Server> backupServer;

    protected final CatchupClientFactory catchupClientFactory;
    protected final DatabaseService databaseService;
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

    public CatchupServersModule( DatabaseService databaseService, PipelineBuilders pipelineBuilders, GlobalModule globalModule )
    {
        this.databaseService = databaseService;
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

        this.catchupClientFactory = createCatchupClient();

        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        PageCache pageCache = globalModule.getPageCache();
        CopiedStoreRecovery copiedStoreRecovery = globalLife.add( new CopiedStoreRecovery( pageCache, fileSystem, globalModule.getStorageEngineFactory() ) );
        this.databaseComponents = new CatchupComponentsService( databaseService, catchupClientFactory, copiedStoreRecovery, fileSystem,
                pageCache, globalConfig, logProvider, globalModule.getGlobalMonitors(), globalModule.getStorageEngineFactory() );
    }

    private CatchupClientFactory createCatchupClient()
    {
        CatchupClientFactory catchupClient = CatchupClientBuilder.builder()
                .defaultDatabaseName( globalConfig.get( GraphDatabaseSettings.active_database ) )
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
            String activeDatabaseName )
    {
        return CatchupServerBuilder.builder()
                .catchupServerHandler( catchupServerHandler )
                .defaultDatabaseName( activeDatabaseName )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( pipelineBuilders.server() )
                .installedProtocolsHandler( installedProtocolsHandler )
                .listenAddress( globalConfig.get( CausalClusteringSettings.transaction_listen_address ) )
                .scheduler( scheduler )
                .bootstrapConfig( serverConfig( globalConfig ) )
                .userLogProvider( userLogProvider )
                .debugLogProvider( logProvider )
                .serverName( "catchup-server" )
                .build();
    }

    protected final Optional<Server> createBackupServer( InstalledProtocolHandler installedProtocolsHandler, CatchupServerHandler catchupServerHandler,
            String activeDatabaseName )
    {
        TransactionBackupServiceProvider transactionBackupServiceProvider =
                new TransactionBackupServiceProvider( logProvider, supportedCatchupProtocols,
                        supportedModifierProtocols,
                        pipelineBuilders.backupServer(),
                        catchupServerHandler,
                        installedProtocolsHandler,
                        activeDatabaseName,
                        scheduler,
                        portRegister );

        return transactionBackupServiceProvider.resolveIfBackupEnabled( globalConfig );
    }

    public final CatchupClientFactory catchupClient()
    {
        return catchupClientFactory;
    }

    public final CatchupComponentsRepository catchupComponents()
    {
        return databaseComponents;
    }

    public final DatabaseService databaseService()
    {
        return databaseService;
    }

    public abstract Server catchupServer();

    public abstract Optional<Server> backupServer();
}
