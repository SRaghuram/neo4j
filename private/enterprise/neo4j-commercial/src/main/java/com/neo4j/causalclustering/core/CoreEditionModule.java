/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.ReplicationModule;
import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.common.DefaultDatabaseService;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.consensus.ConsensusModule;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.core.server.CoreServerModule;
import com.neo4j.causalclustering.core.state.ClusterStateDirectory;
import com.neo4j.causalclustering.core.state.ClusteringModule;
import com.neo4j.causalclustering.core.state.CoreLife;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.CoreStateStorageService;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.diagnostics.CoreMonitor;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.error_handling.PanicEventHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.BetterMessageLogger;
import com.neo4j.causalclustering.logging.MessageLogger;
import com.neo4j.causalclustering.logging.NullMessageLogger;
import com.neo4j.causalclustering.messaging.LoggingOutbound;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.RaftOutbound;
import com.neo4j.causalclustering.messaging.SenderService;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
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
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPluginLoader;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForMultiDC;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForSingleDC;
import com.neo4j.causalclustering.routing.load_balancing.procedure.LegacyGetServersProcedure;
import com.neo4j.causalclustering.routing.multi_cluster.procedure.GetRoutersForAllDatabasesProcedure;
import com.neo4j.causalclustering.routing.multi_cluster.procedure.GetRoutersForDatabaseProcedure;
import com.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.causalclustering.upstream.strategies.TypicallyConnectToRandomReadReplicaStrategy;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import com.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;
import static java.util.Arrays.asList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class CoreEditionModule extends AbstractCoreEditionModule
{
    private final ConsensusModule consensusModule;
    private final ReplicationModule replicationModule;
    private final CoreTopologyService topologyService;
    protected final LogProvider logProvider;
    protected final Config globalConfig;
    private final Supplier<Stream<Pair<AdvertisedSocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;
    private final CoreServerModule coreServerModule;
    private final CoreStateService coreStateService;
    private final DefaultDatabaseService<CoreLocalDatabase> databaseService;
    private final Map<String,DatabaseInitializer> databaseInitializerMap = new HashMap<>();
    //TODO: Find a way to be more generic about this to help with 4.0 plans
    private final String activeDatabaseName;
    private final GlobalModule globalModule;
    private final GlobalTransactionStats globalTransactionStats;

    public CoreEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        final Dependencies globalDependencies = globalModule.getGlobalDependencies();
        final LogService logService = globalModule.getLogService();
        final LifeSupport globalLife = globalModule.getGlobalLife();

        this.globalModule = globalModule;
        this.globalConfig = globalModule.getGlobalConfig();
        this.logProvider = logService.getInternalLogProvider();

        CoreMonitor.register( logProvider, logService.getUserLogProvider(), globalModule.getGlobalMonitors() );

        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        this.activeDatabaseName = globalConfig.get( GraphDatabaseSettings.active_database );
        final DatabaseLayout activeDatabaseLayout = globalModule.getStoreLayout().databaseLayout( activeDatabaseName );

        final File dataDir = globalConfig.get( GraphDatabaseSettings.data_directory );
        /* Database directory is passed here to support migration from earlier versions of cluster state, which were stored *inside* the database directory */
        final ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( fileSystem, dataDir, activeDatabaseLayout.databaseDirectory(), false );
        clusterStateDirectory.initialize();
        globalDependencies.satisfyDependency( clusterStateDirectory );
        CoreStateStorageService storage = new CoreStateStorageService( fileSystem, clusterStateDirectory, globalLife, logProvider, globalConfig );
        storage.migrateIfNecessary( activeDatabaseName );

        boolean wasUnboundOnCreation = !storage.simpleStorage( CORE_MEMBER_ID ).exists();
        globalLife.add( new IdFilesSanitationModule( wasUnboundOnCreation, globalDependencies.provideDependency( DatabaseManager.class ), fileSystem, logProvider ) );

        AvailabilityGuard globalGuard = getGlobalAvailabilityGuard( globalModule.getGlobalClock(), logService, globalConfig );
        threadToTransactionBridge = globalDependencies.satisfyDependency( new ThreadToStatementContextBridge( globalGuard ) );

        final PanicService panicService = new PanicService( logService.getUserLogProvider() );
        // used by test
        globalDependencies.satisfyDependencies( panicService );

        watcherServiceFactory = layout ->
                createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService, fileWatcherFileNameFilter() );

        IdentityModule identityModule = new IdentityModule( globalModule, storage );

        //Build local databases object
        final Supplier<DatabaseManager> databaseManagerSupplier = () -> globalDependencies.resolveDependency( DatabaseManager.class );
        final Supplier<DatabaseHealth> databaseHealthSupplier =
                () -> databaseManagerSupplier.get().getDatabaseContext( globalConfig.get( GraphDatabaseSettings.active_database ) )
                        .map( DatabaseContext::getDependencies )
                        .map( resolver -> resolver.resolveDependency( DatabaseHealth.class ) )
                        .orElseThrow( () -> new IllegalStateException( "Default database not found." ) );

        this.databaseService = createDatabasesService( databaseHealthSupplier, fileSystem, globalAvailabilityGuard, globalModule,
                databaseManagerSupplier, logProvider, globalConfig );
        globalDependencies.satisfyDependency( SslPolicyLoader.create( globalConfig, logProvider ) );

        ClusteringModule clusteringModule = getClusteringModule( globalModule, discoveryServiceFactory, storage, identityModule, globalDependencies );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, logProvider, globalConfig, globalDependencies );

        topologyService = clusteringModule.topologyService();

        long logThresholdMillis = globalConfig.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( globalConfig, logProvider );
        ApplicationSupportedProtocols supportedRaftProtocols = supportedProtocolCreator.getSupportedRaftProtocolsFromConfiguration();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), supportedRaftProtocols );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                asList( new RaftProtocolClientInstallerV2.Factory( pipelineBuilders.client(), logProvider ),
                        new RaftProtocolClientInstallerV1.Factory( pipelineBuilders.client(), logProvider ) ),
                ModifierProtocolInstaller.allClientInstallers );

        Duration handshakeTimeout = globalConfig.get( CausalClusteringSettings.handshake_timeout );
        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilders.client(), handshakeTimeout, logProvider, logService.getUserLogProvider() );
        final SenderService raftSender = new SenderService( channelInitializer, globalModule.getJobScheduler(), logProvider,
                BootstrapConfiguration.clientConfig( globalConfig ) );
        globalLife.add( raftSender );
        this.clientInstalledProtocols = raftSender::installedProtocols;

        final MessageLogger<MemberId> messageLogger = createMessageLogger( globalConfig, globalLife, identityModule.myself() );

        RaftOutbound raftOutbound = new RaftOutbound( topologyService, raftSender, clusteringModule.clusterIdentity(), logProvider, logThresholdMillis );
        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>( raftOutbound, identityModule.myself(), messageLogger );

        consensusModule = new ConsensusModule( identityModule.myself(), globalModule,
                loggingOutbound, clusterStateDirectory.get(), topologyService, storage, activeDatabaseName );

        globalDependencies.satisfyDependency( consensusModule.raftMachine() );
        replicationModule = new ReplicationModule( consensusModule.raftMachine(), identityModule.myself(), globalModule, globalConfig,
                loggingOutbound, storage, logProvider, globalGuard, databaseService );

        StateStorage<Long> lastFlushedStorage = storage.stateStorage( CoreStateFiles.LAST_FLUSHED );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStorage::getInitialState );

        coreStateService = new CoreStateService( identityModule.myself(), globalModule, storage, globalConfig,
                consensusModule.raftMachine(), databaseService, replicationModule, lastFlushedStorage, panicService );

        this.accessCapability = new LeaderCanWrite( consensusModule.raftMachine() );

        InstalledProtocolHandler serverInstalledProtocolHandler = new InstalledProtocolHandler();

        CatchupHandlerFactory handlerFactory = snapshotService -> getHandlerFactory( globalModule, fileSystem, snapshotService );

        this.coreServerModule = new CoreServerModule( identityModule, globalModule, consensusModule, coreStateService, clusteringModule,
                replicationModule, databaseService, databaseHealthSupplier, pipelineBuilders, serverInstalledProtocolHandler,
                handlerFactory, activeDatabaseName, panicService );

        addPanicEventHandlers( globalLife, panicService, databaseHealthSupplier );

        TypicallyConnectToRandomReadReplicaStrategy defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, globalConfig, logProvider, identityModule.myself() );
        UpstreamDatabaseStrategySelector catchupStrategySelector =
                createUpstreamDatabaseStrategySelector( identityModule.myself(), globalConfig, logProvider, topologyService, defaultStrategy );

        CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider =
                new CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider( consensusModule.raftMachine(), topologyService,
                        catchupStrategySelector );
        RaftServerModule.createAndStart( globalModule, consensusModule, identityModule, coreServerModule, pipelineBuilders.server(), messageLogger,
                catchupAddressProvider, supportedRaftProtocols, supportedModifierProtocols, serverInstalledProtocolHandler, activeDatabaseName, panicService );
        serverInstalledProtocols = serverInstalledProtocolHandler::installedProtocols;

        editionInvariants( globalModule, globalDependencies, globalConfig, globalLife );

        globalLife.add( coreServerModule.membershipWaiterLifecycle );

        this.globalTransactionStats = new GlobalTransactionStats();
        initGlobalGuard( globalModule.getGlobalClock(), logService );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        //noinspection deprecation
        globalProcedures.register( new LegacyGetServersProcedure( topologyService, consensusModule.raftMachine(), globalConfig, logProvider ) );

        if ( globalConfig.get( CausalClusteringSettings.multi_dc_license ) )
        {
            globalProcedures.register( new GetServersProcedureForMultiDC( getLoadBalancingProcessor() ) );
        }
        else
        {
            globalProcedures.register( new GetServersProcedureForSingleDC( topologyService, consensusModule.raftMachine(),
                    globalConfig, logProvider ) );
        }

        globalProcedures.register( new GetRoutersForAllDatabasesProcedure( topologyService, globalConfig ) );
        globalProcedures.register( new GetRoutersForDatabaseProcedure( topologyService, globalConfig ) );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
        globalProcedures.register( new CoreRoleProcedure( consensusModule.raftMachine() ) );
        globalProcedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, serverInstalledProtocols ) );
        globalProcedures.registerComponent( Replicator.class, x -> replicationModule.getReplicator(), false );
        globalProcedures.registerProcedure( ReplicationBenchmarkProcedure.class );
    }

    private void addPanicEventHandlers( LifeSupport life, PanicService panicService, Supplier<DatabaseHealth> databaseHealthSupplier )
    {
        // order matters
        panicService.addPanicEventHandler( PanicEventHandlers.raiseAvailabilityGuardEventHandler( globalAvailabilityGuard ) );
        panicService.addPanicEventHandler( PanicEventHandlers.dbHealthEventHandler( databaseHealthSupplier ) );
        panicService.addPanicEventHandler( coreServerModule.commandApplicationProcess() );
        panicService.addPanicEventHandler( consensusModule.raftMachine() );
        panicService.addPanicEventHandler( PanicEventHandlers.disableServerEventHandler( coreServerModule.catchupServer() ) );
        coreServerModule.backupServer().ifPresent( server -> panicService.addPanicEventHandler( PanicEventHandlers.disableServerEventHandler( server ) ) );
        panicService.addPanicEventHandler( PanicEventHandlers.shutdownLifeCycle( life ) );
    }

    private LoadBalancingProcessor getLoadBalancingProcessor()
    {
        try
        {
            return LoadBalancingPluginLoader.load( topologyService, consensusModule.raftMachine(), logProvider, globalConfig );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return SchemaWriteGuard.ALLOW_ALL_WRITES;
    }

    @Override
    public TransactionCounters globalTransactionCounter()
    {
        return globalTransactionStats;
    }

    @Override
    ConsensusModule consensusModule()
    {
        return consensusModule;
    }

    @Override
    CoreStateService coreStateComponents()
    {
        return coreStateService;
    }

    @Override
    void disableCatchupServer() throws Throwable
    {
        coreServerModule.catchupServer().disable();
    }

    public boolean isLeader()
    {
        return consensusModule.raftMachine().currentRole() == Role.LEADER;
    }

    /**
     * Returns {@code true} because {@link DatabaseManager}'s lifecycle is managed by {@link DatabaseService} via {@link CoreLife}.
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
    public EditionDatabaseContext createDatabaseContext( String databaseName )
    {
        return new CoreDatabaseContext( globalModule, this, databaseName );
    }

    private DefaultDatabaseService<CoreLocalDatabase> createDatabasesService( Supplier<DatabaseHealth> databaseHealthSupplier,
            FileSystemAbstraction fileSystem, AvailabilityGuard availabilityGuard, GlobalModule globalModule,
            Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, Config config )
    {
        return new DefaultDatabaseService<>( CoreLocalDatabase::new, databaseManagerSupplier, globalModule.getStoreLayout(),
                availabilityGuard, databaseHealthSupplier, fileSystem, globalModule.getPageCache(), logProvider, config );
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

    protected CatchupServerHandler getHandlerFactory( GlobalModule globalModule,
            FileSystemAbstraction fileSystem, CoreSnapshotService snapshotService )
    {
        Supplier<DatabaseManager> databaseManagerSupplier = globalModule.getGlobalDependencies().provideDependency( DatabaseManager.class );
        return new MultiDatabaseCatchupServerHandler( databaseManagerSupplier, logProvider, fileSystem, snapshotService );
    }

    protected ClusteringModule getClusteringModule( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory,
            CoreStateStorageService storage, IdentityModule identityModule, Dependencies dependencies )
    {
        SslPolicyLoader sslPolicyFactory = dependencies.resolveDependency( SslPolicyLoader.class );
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( globalConfig.get( CausalClusteringSettings.ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslDiscoveryServiceFactory )
        {
            ((SslDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }

        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(), globalModule, storage, databaseService,
                databaseInitializerMap::get );
    }

    @Override
    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, GlobalModule platform, AbstractEditionModule edition,
            GlobalProcedures globalProcedures, Logger msgLog )
    {
        return new MultiDatabaseManager( platform, edition, globalProcedures, msgLog, graphDatabaseFacade );
    }

    @Override
    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        createCommercialEditionDatabases( databaseManager, config );
    }

    private void createCommercialEditionDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, SYSTEM_DATABASE_NAME );
        createConfiguredDatabases( databaseManager, config );
    }

    private void createConfiguredDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, config.get( GraphDatabaseSettings.active_database ) );
    }

    protected void createDatabase( DatabaseManager databaseManager, String databaseName )
    {
        try
        {
            CoreLocalDatabase db = databaseService.registerDatabase( databaseName );
            coreStateService.create( db );
            databaseManager.createDatabase( databaseName );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }

    @Override
    public DatabaseTransactionStats createTransactionMonitor()
    {
        return globalTransactionStats.createDatabaseTransactionMonitor();
    }

    @Override
    public AvailabilityGuard getGlobalAvailabilityGuard( Clock clock, LogService logService, Config config )
    {
        initGlobalGuard( clock, logService );
        return globalAvailabilityGuard;
    }

    @Override
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName, Clock clock, LogService logService, Config config )
    {
        return ((CompositeDatabaseAvailabilityGuard) getGlobalAvailabilityGuard( clock, logService, config )).createDatabaseAvailabilityGuard( databaseName );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule, GlobalProcedures globalProcedures )
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
            securityProvider = EnterpriseNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }

    private void initGlobalGuard( Clock clock, LogService logService )
    {
        if ( globalAvailabilityGuard == null )
        {
            globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( clock, logService );
        }
    }
}
