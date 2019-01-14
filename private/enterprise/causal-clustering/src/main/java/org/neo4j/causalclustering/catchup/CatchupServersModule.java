/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.util.Collection;
import java.util.Optional;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.PipelineBuilders;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import org.neo4j.causalclustering.net.InstalledProtocolHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

public abstract class CatchupServersModule
{
    protected Server catchupServer;
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    protected Optional<Server> backupServer;

    protected final CatchupClientFactory catchupClientFactory;
    protected final DatabaseService databaseService;
    protected final LogProvider logProvider;
    protected final Config config;
    protected final LifeSupport lifeSupport;

    private final ApplicationSupportedProtocols supportedCatchupProtocols;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final PipelineBuilders pipelineBuilders;
    private final CatchupComponentsService databaseComponents;
    private final LogProvider userLogProvider;

    public CatchupServersModule( DatabaseService databaseService, PipelineBuilders pipelineBuilders, PlatformModule platformModule )
    {
        this.databaseService = databaseService;
        this.pipelineBuilders = pipelineBuilders;

        this.logProvider = platformModule.logging.getInternalLogProvider();
        this.userLogProvider = platformModule.logging.getUserLogProvider();
        this.config = platformModule.config;
        this.lifeSupport = platformModule.life;

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        this.supportedCatchupProtocols = supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration();
        this.supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        this.catchupClientFactory = createCatchupClient();

        CopiedStoreRecovery copiedStoreRecovery = lifeSupport.add( new CopiedStoreRecovery( config, platformModule.kernelExtensionFactories,
                platformModule.pageCache ) );
        this.databaseComponents = new CatchupComponentsService( databaseService, catchupClientFactory, copiedStoreRecovery,
                platformModule.fileSystem, platformModule.pageCache, config, logProvider );
    }

    private CatchupClientFactory createCatchupClient()
    {
        CatchupClientFactory catchupClient = CatchupClientBuilder.builder()
                .defaultDatabaseName( config.get( GraphDatabaseSettings.active_database ) )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( pipelineBuilders.client() )
                .inactivityTimeout( config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ) )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .debugLogProvider( logProvider )
                .userLogProvider( userLogProvider )
                .build();
        lifeSupport.add( catchupClient );
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
                .listenAddress( config.get( CausalClusteringSettings.transaction_listen_address ) )
                .userLogProvider( userLogProvider )
                .debugLogProvider( logProvider )
                .serverName( "catchup-server" )
                .build();
    }

    protected final Optional<Server> createBackupServer( InstalledProtocolHandler installedProtocolsHandler, CatchupServerHandler catchupServerHandler,
            String activeDatabaseName )
    {
        TransactionBackupServiceProvider transactionBackupServiceProvider =
                new TransactionBackupServiceProvider( logProvider,
                        userLogProvider,
                        supportedCatchupProtocols,
                        supportedModifierProtocols,
                        pipelineBuilders.backupServer(),
                        catchupServerHandler,
                        installedProtocolsHandler,
                        activeDatabaseName );

        return transactionBackupServiceProvider.resolveIfBackupEnabled( config );
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
