/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.consensus.ConsensusModule;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.core.server.CoreServerModule;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.ClusteringModule;
import com.neo4j.causalclustering.core.state.CoreLife;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.diagnostics.CoreMonitor;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.error_handling.PanicEventHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.BetterMessageLogger;
import com.neo4j.causalclustering.logging.MessageLogger;
import com.neo4j.causalclustering.logging.NullMessageLogger;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.causalclustering.messaging.LoggingOutbound;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.RaftChannelPoolService;
import com.neo4j.causalclustering.messaging.RaftOutbound;
import com.neo4j.causalclustering.messaging.RaftSender;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.routing.load_balancing.DefaultLeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.multi_cluster.procedure.GetRoutersForAllDatabasesProcedure;
import com.neo4j.causalclustering.routing.multi_cluster.procedure.GetRoutersForDatabaseProcedure;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.TypicallyConnectToRandomReadReplicaStrategy;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.CompositeDatabaseHealth;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInProcedures;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.Clocks;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.recovery.Recovery.recoveryFacade;

/**
 * This implementation of {@link AbstractEditionModule} creates the service instances
 * which are specific to the Core members of a causal cluster.
 */
public class CoreEditionModule extends AbstractCoreEditionModule
{
    private final IdentityModule identityModule;
    private final SslPolicyLoader sslPolicyLoader;
    private final RaftChannelPoolService raftChannelPoolService;
    private final CoreStateStorageFactory storageFactory;
    private final ClusterStateLayout clusterStateLayout;
    private final CatchupComponentsProvider catchupComponentsProvider;
    private ConsensusModule consensusModule;
    private ReplicationModule replicationModule;
    private CoreServerModule coreServerModule;
    private CoreTopologyService topologyService;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final PanicService panicService;

    private CoreStateService coreStateService;
    private final StartupCoreStateCheck startupCoreStateCheck;

    private final PipelineBuilders pipelineBuilders;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;
    private final ApplicationSupportedProtocols supportedRaftProtocols;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final InstalledProtocolHandler serverInstalledProtocolHandler;

    private final Map<String,DatabaseInitializer> databaseInitializerMap = new HashMap<>();
    private final CompositeDatabaseAvailabilityGuard globalGuard;
    private final CompositeDatabaseHealth globalHealth;
    private final LogProvider logProvider;
    private final Config globalConfig;
    //TODO: remove as soon as independent lifecycle is in place
    private final String defaultDatabaseName;
    private final GlobalModule globalModule;

    public CoreEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        final Dependencies globalDependencies = globalModule.getGlobalDependencies();
        final LogService logService = globalModule.getLogService();
        final LifeSupport globalLife = globalModule.getGlobalLife();

        this.globalModule = globalModule;
        this.globalConfig = globalModule.getGlobalConfig();
        this.globalGuard = globalModule.getGlobalAvailabilityGuard();
        this.globalHealth = globalModule.getGlobalHealthService();
        this.logProvider = logService.getInternalLogProvider();

        CoreMonitor.register( logProvider, logService.getUserLogProvider(), globalModule.getGlobalMonitors() );

        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        this.defaultDatabaseName = globalConfig.get( GraphDatabaseSettings.default_database );

        final File dataDir = globalConfig.get( GraphDatabaseSettings.data_directory );
        clusterStateLayout = ClusterStateLayout.of( dataDir );
        globalDependencies.satisfyDependency( clusterStateLayout );
        storageFactory = new CoreStateStorageFactory( fileSystem, clusterStateLayout, logProvider, globalConfig );

        startupCoreStateCheck = new StartupCoreStateCheck( fileSystem, clusterStateLayout ); // must be constructed before storage is touched by other modules

        threadToTransactionBridge = globalDependencies.satisfyDependency( new ThreadToStatementContextBridge() );

        panicService = new PanicService( logService.getUserLogProvider() );
        globalDependencies.satisfyDependencies( panicService ); // used by test

        watcherServiceFactory = layout ->
                createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService, fileWatcherFileNameFilter() );

        identityModule = new IdentityModule( globalModule, storageFactory );
        this.discoveryServiceFactory = discoveryServiceFactory;

        sslPolicyLoader = SslPolicyLoader.create( globalConfig, logProvider );
        globalDependencies.satisfyDependency( sslPolicyLoader );

        pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, globalConfig, sslPolicyLoader );

        catchupComponentsProvider = new CatchupComponentsProvider( globalModule, pipelineBuilders );
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( globalConfig, logProvider );
        supportedRaftProtocols = supportedProtocolCreator.getSupportedRaftProtocolsFromConfiguration();
        supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), supportedRaftProtocols );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                List.of( new RaftProtocolClientInstallerV2.Factory( pipelineBuilders.client(), logProvider ) ),
                ModifierProtocolInstaller.allClientInstallers );

        Duration handshakeTimeout = globalConfig.get( CausalClusteringSettings.handshake_timeout );
        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilders.client(), handshakeTimeout, logProvider, logService.getUserLogProvider() );
        raftChannelPoolService = new RaftChannelPoolService( BootstrapConfiguration.clientConfig( globalConfig ), globalModule.getJobScheduler(), logProvider,
                        channelInitializer );
        globalLife.add( raftChannelPoolService );
        this.clientInstalledProtocols = raftChannelPoolService::installedProtocols;
        serverInstalledProtocolHandler = new InstalledProtocolHandler();
        serverInstalledProtocols = serverInstalledProtocolHandler::installedProtocols;

        editionInvariants( globalModule, globalDependencies, globalConfig, globalLife );
    }

    private void addCoreServerComponentsToLifecycle( CoreServerModule coreServerModule,
            LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> raftMessageHandlerChain, LifeSupport lifeSupport )
    {
        RecoveryFacade recoveryFacade = recoveryFacade( globalModule.getFileSystem(), globalModule.getPageCache(), globalConfig,
                globalModule.getStorageEngineFactory() );

        lifeSupport.add( coreServerModule.createCoreLife( raftMessageHandlerChain, logProvider, recoveryFacade ) );
        lifeSupport.add( coreServerModule.catchupServer() ); // must start last and stop first, since it handles external requests
        coreServerModule.backupServer().ifPresent( lifeSupport::add );
        lifeSupport.add( coreServerModule.downloadService() );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new GetRoutersForAllDatabasesProcedure( topologyService, globalConfig ) );
        globalProcedures.register( new GetRoutersForDatabaseProcedure( topologyService, globalConfig ) );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
        globalProcedures.register( new CoreRoleProcedure( consensusModule.raftMachine() ) );
        globalProcedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, serverInstalledProtocols ) );
        globalProcedures.registerComponent( Replicator.class, x -> replicationModule.getReplicator(), false );
        globalProcedures.registerProcedure( ReplicationBenchmarkProcedure.class );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        RaftMachine raftMachine = consensusModule.raftMachine();
        LeaderService leaderService = new DefaultLeaderService( raftMachine, topologyService );
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        return new CoreRoutingProcedureInstaller( topologyService, leaderService, config, logProvider );
    }

    private void addPanicEventHandlers( LifeSupport life, PanicService panicService )
    {
        // order matters
        panicService.addPanicEventHandler( PanicEventHandlers.raiseAvailabilityGuardEventHandler( globalModule.getGlobalAvailabilityGuard() ) );
        panicService.addPanicEventHandler( PanicEventHandlers.dbHealthEventHandler( globalHealth ) );
        panicService.addPanicEventHandler( coreServerModule.commandApplicationProcess() );
        panicService.addPanicEventHandler( consensusModule.raftMachine() );
        panicService.addPanicEventHandler( PanicEventHandlers.disableServerEventHandler( coreServerModule.catchupServer() ) );
        coreServerModule.backupServer().ifPresent( server -> panicService.addPanicEventHandler( PanicEventHandlers.disableServerEventHandler( server ) ) );
        panicService.addPanicEventHandler( PanicEventHandlers.shutdownLifeCycle( life ) );
    }

    @Override
    CoreStateService coreStateComponents()
    {
        return coreStateService;
    }

    public boolean isLeader()
    {
        return consensusModule.raftMachine().currentRole() == Role.LEADER;
    }

    /**
     * Returns {@code true} because {@link DatabaseManager}'s lifecycle is managed by {@link ClusteredDatabaseManager} via {@link CoreLife}.
     * So {@link DatabaseManager} does not need to be included in the global lifecycle.
     *
     * @return always {@code true}.
     */
    @Override
    public boolean handlesDatabaseManagerLifecycle()
    {
        return true;
    }

    /* Component Factories */
    @Override
    public EditionDatabaseComponents createDatabaseComponents( DatabaseId databaseId )
    {
        return new CoreDatabaseComponents( globalModule, this, databaseId );
    }

    private static MessageLogger<MemberId> createMessageLogger( Config config, LifeSupport life, MemberId myself )
    {
        final MessageLogger<MemberId> messageLogger;
        if ( config.get( CausalClusteringSettings.raft_messages_log_enable ) )
        {
            File logFile = config.get( CausalClusteringSettings.raft_messages_log_path );
            messageLogger = life.add( new BetterMessageLogger<>( myself, raftMessagesLog( logFile ), Clocks.systemClock() ) );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }
        return messageLogger;
    }

    private static PrintWriter raftMessagesLog( File logFile )
    {
        //noinspection ResultOfMethodCallIgnored
        logFile.getParentFile().mkdirs();
        try
        {
            return new PrintWriter( new FileOutputStream( logFile, true ) );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private UpstreamDatabaseStrategySelector createUpstreamDatabaseStrategySelector( MemberId myself, Config config, LogProvider logProvider,
            TopologyService topologyService, UpstreamDatabaseSelectionStrategy defaultStrategy )
    {
        UpstreamDatabaseStrategiesLoader loader;
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            loader = new UpstreamDatabaseStrategiesLoader( topologyService, config, myself, logProvider );
            logProvider.getLog( getClass() ).info( "Multi-Data Center option enabled." );
        }
        else
        {
            loader = new NoOpUpstreamDatabaseStrategiesLoader();
        }

        return new UpstreamDatabaseStrategySelector( defaultStrategy, loader, logProvider );
    }

    private CatchupServerHandler getHandlerFactory( FileSystemAbstraction fileSystem,
            CoreSnapshotService snapshotService, DatabaseManager<CoreDatabaseContext> databaseManager )
    {
        return new MultiDatabaseCatchupServerHandler( databaseManager, logProvider, fileSystem, snapshotService );
    }

    private ClusteringModule getClusteringModule( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory,
            CoreStateStorageFactory storageFactory, ClusteredMultiDatabaseManager<CoreDatabaseContext> databaseManager,
            IdentityModule identityModule, SslPolicyLoader sslPolicyLoader )
    {
        TemporaryDatabaseFactory temporaryDatabaseFactory = new CommercialTemporaryDatabaseFactory( globalModule.getPageCache() );
        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(), globalModule, storageFactory, databaseManager, temporaryDatabaseFactory,
                sslPolicyLoader, dbName -> databaseInitializerMap.getOrDefault( dbName, DatabaseInitializer.NO_INITIALIZATION ) );
    }

    private void createDatabaseManagerDependentModules( final ClusteredMultiDatabaseManager<CoreDatabaseContext> databaseManager )
    {
        final LifeSupport globalLife = globalModule.getGlobalLife();
        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();

        globalLife.add( new IdFilesSanitationModule( startupCoreStateCheck, databaseManager, fileSystem, logProvider ) );

        ClusteringModule clusteringModule = getClusteringModule( globalModule, discoveryServiceFactory, storageFactory, databaseManager,
                identityModule, sslPolicyLoader );

        topologyService = clusteringModule.topologyService();

        long logThresholdMillis = globalConfig.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        final MessageLogger<MemberId> messageLogger = createMessageLogger( globalConfig, globalLife, identityModule.myself() );

        RaftMessageDispatcher raftMessageDispatcher = new RaftMessageDispatcher( logProvider, globalModule.getGlobalClock() );
        RaftSender raftSender = new RaftSender( logProvider, raftChannelPoolService );
        RaftOutbound raftOutbound = new RaftOutbound( topologyService, raftSender, raftMessageDispatcher,
                clusteringModule.clusterIdentity(), logProvider, logThresholdMillis, identityModule.myself(), globalModule.getGlobalClock() );
        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>( raftOutbound, identityModule.myself(), messageLogger );

        consensusModule = new ConsensusModule( identityModule.myself(), globalModule, loggingOutbound, clusterStateLayout, topologyService,
                storageFactory, defaultDatabaseName );

        replicationModule = new ReplicationModule( consensusModule.raftMachine(), identityModule.myself(), globalModule, globalConfig,
                loggingOutbound, storageFactory, logProvider, globalGuard, databaseManager, defaultDatabaseName );

        StateStorage<Long> lastFlushedStateStorage = storageFactory.createLastFlushedStorage( defaultDatabaseName, globalLife );

        coreStateService = new CoreStateService( identityModule.myself(), globalModule, storageFactory, globalConfig,
                consensusModule.raftMachine(), databaseManager, replicationModule, lastFlushedStateStorage, panicService );

        CatchupHandlerFactory handlerFactory = snapshotService -> getHandlerFactory( fileSystem, snapshotService, databaseManager );

        //TODO:
        //  - Implement start stop etc... for databases and make sure store-copy catchup machinery uses it
        //  - plan how to test independent lifecycle
        coreServerModule = new CoreServerModule( identityModule, globalModule, consensusModule, coreStateService, clusteringModule,
                replicationModule, databaseManager, globalHealth, catchupComponentsProvider, serverInstalledProtocolHandler,
                handlerFactory, panicService );
        globalModule.getGlobalDependencies().satisfyDependency( coreServerModule );

        TypicallyConnectToRandomReadReplicaStrategy defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, globalConfig, logProvider, identityModule.myself() );

        UpstreamDatabaseStrategySelector catchupStrategySelector =
                createUpstreamDatabaseStrategySelector( identityModule.myself(), globalConfig, logProvider, topologyService, defaultStrategy );

        CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider =
                new CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider( consensusModule.raftMachine(), topologyService,
                        catchupStrategySelector );

        globalModule.getGlobalDependencies().satisfyDependency( consensusModule.raftMachine() );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStateStorage::getInitialState );
        accessCapability = new LeaderCanWrite( consensusModule.raftMachine() );

        RaftMessageHandlerChainFactory raftMessageHandlerChainFactory = new RaftMessageHandlerChainFactory( globalModule, raftMessageDispatcher,
                catchupAddressProvider, panicService );
        LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> raftMessageHandlerChain =
                raftMessageHandlerChainFactory.createMessageHandlerChain( consensusModule, coreServerModule );

        RaftServerFactory raftServerFactory = new RaftServerFactory( globalModule, identityModule, pipelineBuilders.server(), messageLogger,
                supportedRaftProtocols, supportedModifierProtocols );
        Server raftServer = raftServerFactory.createRaftServer( raftMessageDispatcher, serverInstalledProtocolHandler );
        globalModule.getGlobalDependencies().satisfyDependencies( raftServer ); // resolved in tests
        globalLife.add( raftServer );

        // needs to be after Raft server in the lifecycle
        addCoreServerComponentsToLifecycle( coreServerModule, raftMessageHandlerChain, globalLife );

        addPanicEventHandlers( globalLife, panicService );
        globalLife.add( coreServerModule.membershipWaiterLifecycle() );
    }

    public DatabaseManager<CoreDatabaseContext> createDatabaseManager( GraphDatabaseFacade facade, GlobalModule platform, Log log )
    {
        ClusteredMultiDatabaseManager<CoreDatabaseContext> databaseManager = new CoreDatabaseManager( platform, this, log, facade,
                this::coreStateComponents, catchupComponentsProvider::createDatabaseComponents, globalModule.getGlobalAvailabilityGuard(),
                platform.getFileSystem(), platform.getPageCache(), logProvider, platform.getGlobalConfig(), globalHealth );
        createDatabaseManagerDependentModules( databaseManager );
        return databaseManager;
    }

    @Override
    public void createDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        createCommercialEditionDatabases( databaseManager, config );
    }

    private void createCommercialEditionDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        createDatabase( databaseManager, SYSTEM_DATABASE_NAME );
        createConfiguredDatabases( databaseManager, config );
    }

    private void createConfiguredDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        createDatabase( databaseManager, config.get( GraphDatabaseSettings.default_database ) );
    }

    private void createDatabase( DatabaseManager<?> databaseManager, String databaseName ) throws DatabaseExistsException
    {
        databaseManager.createDatabase( new DatabaseId( databaseName ) );
    }

    private DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        SecurityProvider securityProvider;
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            CommercialSecurityModule securityModule = (CommercialSecurityModule) setupSecurityModule( globalModule, this,
                    globalModule.getLogService().getUserLog( CoreEditionModule.class ), globalProcedures, "commercial-security-module" );
            securityModule.getDatabaseInitializer().ifPresent( dbInit -> databaseInitializerMap.put( SYSTEM_DATABASE_NAME, dbInit ) );
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = CommercialNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }
}
