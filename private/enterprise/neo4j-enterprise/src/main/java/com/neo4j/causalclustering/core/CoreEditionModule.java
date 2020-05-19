/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServerProvider;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.RaftGroupFactory;
import com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferService;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v3.RaftProtocolClientInstallerV3;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.ClusterStateMigrator;
import com.neo4j.causalclustering.core.state.DiscoveryModule;
import com.neo4j.causalclustering.diagnostics.GlobalTopologyStateDiagnosticProvider;
import com.neo4j.causalclustering.diagnostics.RaftMonitor;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.member.DefaultDiscoveryMemberFactory;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.BetterRaftMessageLogger;
import com.neo4j.causalclustering.logging.NullRaftMessageLogger;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import com.neo4j.causalclustering.messaging.RaftChannelPoolService;
import com.neo4j.causalclustering.messaging.RaftSender;
import com.neo4j.causalclustering.monitoring.ThroughputMonitorService;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.init.ClientChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import com.neo4j.causalclustering.routing.load_balancing.DefaultLeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.ClusteredDbmsReconcilerModule;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.EnterpriseSystemGraphComponent;
import com.neo4j.dbms.SystemDbOnlyReplicatedDatabaseEventService;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.dbms.procedures.ClusteredDatabaseStateProcedure;
import com.neo4j.enterprise.edition.AbstractEnterpriseEditionModule;
import com.neo4j.fabric.bootstrap.EnterpriseFabricServicesBootstrap;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;
import com.neo4j.procedure.enterprise.builtin.SettingsWhitelist;
import com.neo4j.server.enterprise.EnterpriseNeoWebServer;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.cypher.internal.javacompat.EnterpriseCypherEngineProvider;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.scheduler.Group;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.recovery.Recovery.recoveryFacade;

/**
 * This implementation of {@link AbstractEditionModule} creates the service instances
 * which are specific to the Core members of a causal cluster.
 */
public class CoreEditionModule extends ClusteringEditionModule implements AbstractEnterpriseEditionModule
{
    private final IdentityModule identityModule;
    private final SslPolicyLoader sslPolicyLoader;
    private final ClusterStateStorageFactory storageFactory;
    private final ClusterStateLayout clusterStateLayout;
    private final CatchupComponentsProvider catchupComponentsProvider;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final PanicService panicService;

    private final PipelineBuilders pipelineBuilders;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;
    private final ApplicationSupportedProtocols supportedRaftProtocols;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final InstalledProtocolHandler serverInstalledProtocolHandler;

    private final LogProvider logProvider;
    private final Config globalConfig;
    private final GlobalModule globalModule;
    private final EnterpriseTemporaryDatabaseFactory temporaryDatabaseFactory;
    private final RaftSender raftSender;

    private final EnterpriseFabricServicesBootstrap fabricServicesBootstrap;

    private CoreDatabaseFactory coreDatabaseFactory;
    private CoreTopologyService topologyService;
    private DatabaseStartAborter databaseStartAborter;
    private ClusteredDbmsReconcilerModule reconcilerModule;
    private LeaderService leaderService;

    public CoreEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        super( globalModule );

        final Dependencies globalDependencies = globalModule.getGlobalDependencies();
        final LogService logService = globalModule.getLogService();
        final LifeSupport globalLife = globalModule.getGlobalLife();

        this.globalModule = globalModule;
        this.globalConfig = globalModule.getGlobalConfig();
        this.logProvider = logService.getInternalLogProvider();

        SettingsWhitelist settingsWhiteList = new SettingsWhitelist( globalConfig );
        globalDependencies.satisfyDependency( settingsWhiteList );

        RaftMonitor.register( logService, globalModule.getGlobalMonitors(), globalModule.getGlobalClock() );

        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();

        final File dataDir = globalConfig.get( GraphDatabaseSettings.data_directory ).toFile();
        clusterStateLayout = ClusterStateLayout.of( dataDir );
        globalDependencies.satisfyDependency( clusterStateLayout );
        storageFactory = new ClusterStateStorageFactory( fileSystem, clusterStateLayout, logProvider, globalConfig,
                globalModule.getOtherMemoryPool().getPoolMemoryTracker() );

        // migration needs to happen as early as possible in the lifecycle
        var clusterStateMigrator = createClusterStateMigrator( globalModule, clusterStateLayout, storageFactory );
        globalLife.add( clusterStateMigrator );

        temporaryDatabaseFactory = new EnterpriseTemporaryDatabaseFactory( globalModule.getPageCache(), globalModule.getFileSystem() );

        panicService = new PanicService( globalModule.getJobScheduler(), logService );
        globalDependencies.satisfyDependencies( panicService ); // used by test

        watcherServiceFactory = layout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService, fileWatcherFileNameFilter() );

        identityModule = new IdentityModule( globalModule, storageFactory );
        this.discoveryServiceFactory = discoveryServiceFactory;

        sslPolicyLoader = SslPolicyLoader.create( globalConfig, logProvider );
        globalDependencies.satisfyDependency( sslPolicyLoader );

        pipelineBuilders = new PipelineBuilders( sslPolicyLoader );

        catchupComponentsProvider = new CatchupComponentsProvider( globalModule, pipelineBuilders );
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( globalConfig, logProvider );
        supportedRaftProtocols = supportedProtocolCreator.getSupportedRaftProtocolsFromConfiguration();
        supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        RaftChannelPoolService raftChannelPoolService = buildRaftChannelPoolService( globalModule );
        globalLife.add( raftChannelPoolService );

        this.clientInstalledProtocols = raftChannelPoolService::installedProtocols;
        serverInstalledProtocolHandler = new InstalledProtocolHandler();
        serverInstalledProtocols = serverInstalledProtocolHandler::installedProtocols;

        this.raftSender = new RaftSender( logProvider, raftChannelPoolService );

        satisfyEnterpriseOnlyDependencies( this.globalModule );

        editionInvariants( globalModule, globalDependencies );

        fabricServicesBootstrap = new EnterpriseFabricServicesBootstrap.Core( globalLife, globalDependencies, logService );

        setGlobalRaftParallelism( globalModule, globalConfig );
    }

    private static void setGlobalRaftParallelism( GlobalModule globalModule, Config globalConfig )
    {
        globalModule.getJobScheduler().setParallelism( Group.CORE_STATE_APPLIER, globalConfig.get( CausalClusteringSettings.command_applier_parallelism ) );
        globalModule.getJobScheduler()
                .setParallelism( Group.RAFT_HANDLER, globalConfig.get( CausalClusteringSettings.raft_handler_parallelism ) );
    }

    private void createCoreServers( LifeSupport life, DatabaseManager<?> databaseManager, FileSystemAbstraction fileSystem )
    {
        int maxChunkSize = globalConfig.get( CausalClusteringSettings.store_copy_chunk_size );
        CatchupServerHandler catchupServerHandler = new MultiDatabaseCatchupServerHandler( databaseManager, fileSystem, maxChunkSize, logProvider );
        Server catchupServer = catchupComponentsProvider.createCatchupServer( serverInstalledProtocolHandler, catchupServerHandler );
        life.add( catchupServer );
        // used by ReadReplicaHierarchicalCatchupIT
        globalModule.getGlobalDependencies().satisfyDependencies( (CatchupServerProvider) () -> catchupServer );

        Optional<Server> optionalBackupServer = catchupComponentsProvider.createBackupServer( serverInstalledProtocolHandler, catchupServerHandler );
        if ( optionalBackupServer.isPresent() )
        {
            Server backupServer = optionalBackupServer.get();
            life.add( backupServer );
        }
    }

    @Override
    public QueryEngineProvider getQueryEngineProvider()
    {
        // Clustering is enterprise only
        return new EnterpriseCypherEngineProvider();
    }

    @Override
    public EditionDatabaseComponents createDatabaseComponents( NamedDatabaseId namedDatabaseId )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService, databaseManager.databaseIdRepository() ) );
        globalProcedures.register( new CoreRoleProcedure( databaseManager ) );
        globalProcedures.register( new ClusteredDatabaseStateProcedure( databaseManager.databaseIdRepository(), topologyService,
                reconcilerModule.databaseStateService() ) );
        globalProcedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, serverInstalledProtocols ) );
        // TODO: Figure out how the replication benchmark procedure should work.
//        globalProcedures.registerComponent( Replicator.class, x -> replicationModule.getReplicator(), false );
//        globalProcedures.registerProcedure( ReplicationBenchmarkProcedure.class );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        Config config = globalModule.getGlobalConfig();
        return new CoreRoutingProcedureInstaller( topologyService, leaderService, databaseManager, config, logProvider );
    }

    /* Component Factories */
    private static RaftMessageLogger<MemberId> createRaftLogger( GlobalModule globalModule, MemberId myself )
    {
        RaftMessageLogger<MemberId> raftMessageLogger;
        var config = globalModule.getGlobalConfig();
        if ( config.get( CausalClusteringSettings.raft_messages_log_enable ) )
        {
            var logFile = config.get( CausalClusteringSettings.raft_messages_log_path ).toFile();
            var logger = new BetterRaftMessageLogger<>( myself, logFile, globalModule.getFileSystem(), globalModule.getGlobalClock() );
            raftMessageLogger = globalModule.getGlobalLife().add( logger );
        }
        else
        {
            raftMessageLogger = new NullRaftMessageLogger<>();
        }
        return raftMessageLogger;
    }

    private void createDatabaseManagerDependentModules( final CoreDatabaseManager databaseManager )
    {
        var databaseEventService = new SystemDbOnlyReplicatedDatabaseEventService( logProvider );
        var globalLife = globalModule.getGlobalLife();
        var fileSystem = globalModule.getFileSystem();
        var myIdentity = identityModule.myself();

        Supplier<GraphDatabaseService> systemDbSupplier = () -> databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow().databaseFacade();
        var dbmsModel = new ClusterSystemGraphDbmsModel( systemDbSupplier );

        reconcilerModule = new ClusteredDbmsReconcilerModule( globalModule, databaseManager, databaseEventService, storageFactory,
                reconciledTxTracker, dbmsModel );

        databaseStartAborter = new DatabaseStartAborter( globalModule.getGlobalAvailabilityGuard(), dbmsModel, globalModule.getGlobalClock(),
                Duration.ofSeconds( 5 ) );

        var dependencies = globalModule.getGlobalDependencies();
        dependencies.satisfyDependencies( databaseEventService );
        dependencies.satisfyDependency( reconciledTxTracker );

        topologyService = createTopologyService( myIdentity, databaseManager, reconcilerModule.databaseStateService() );
        dependencies.satisfyDependency( new GlobalTopologyStateDiagnosticProvider( topologyService ) );
        reconcilerModule.registerDatabaseStateChangedListener( topologyService );

        leaderService = new DefaultLeaderService( topologyService, logProvider );
        dependencies.satisfyDependencies( leaderService );

        RaftMessageLogger<MemberId> raftLogger = createRaftLogger( globalModule, myIdentity );

        RaftMessageDispatcher raftMessageDispatcher = new RaftMessageDispatcher( logProvider, globalModule.getGlobalClock() );

        var globalOtherTracker = globalModule.getOtherMemoryPool().getPoolMemoryTracker();
        var leaderTransferInterval = globalConfig.get( CausalClusteringSettings.leader_transfer_interval );
        var leaderTransferBackoff = globalConfig.get( CausalClusteringSettings.leader_transfer_member_backoff );

        var leaderTransferService = new LeaderTransferService( globalModule.getJobScheduler(), globalConfig, leaderTransferInterval, databaseManager,
                raftMessageDispatcher, myIdentity, leaderTransferBackoff, logProvider, globalModule.getGlobalClock(), leaderService );

        RaftGroupFactory raftGroupFactory = new RaftGroupFactory( myIdentity, globalModule, clusterStateLayout, topologyService, storageFactory,
                leaderTransferService, namedDatabaseId -> ((DefaultLeaderService) leaderService).createListener( namedDatabaseId ), globalOtherTracker );

        RecoveryFacade recoveryFacade = recoveryFacade( globalModule.getFileSystem(), globalModule.getPageCache(), globalModule.getTracers(), globalConfig,
                globalModule.getStorageEngineFactory(), globalOtherTracker );

        addThroughputMonitorService();

        this.coreDatabaseFactory = new CoreDatabaseFactory( globalModule, panicService, databaseManager, topologyService, storageFactory,
                temporaryDatabaseFactory, myIdentity, raftGroupFactory, raftMessageDispatcher, catchupComponentsProvider,
                recoveryFacade, raftLogger, raftSender, databaseEventService, dbmsModel, databaseStartAborter );

        RaftServerFactory raftServerFactory = new RaftServerFactory( globalModule, identityModule, pipelineBuilders.server(), raftLogger,
                supportedRaftProtocols, supportedModifierProtocols );

        Server raftServer = raftServerFactory.createRaftServer( raftMessageDispatcher, serverInstalledProtocolHandler );
        globalModule.getGlobalDependencies().satisfyDependencies( raftServer ); // resolved in tests
        globalLife.add( raftServer );

        createCoreServers( globalLife, databaseManager, fileSystem );

        // Reconciler module starts actual databases, which depend on all of the above components at runtime.
        globalLife.add( reconcilerModule );

        // LeaderTransferService is not required by databases to function. It monitors the databases running on this machine.
        globalLife.add( leaderTransferService );

    }

    private void addThroughputMonitorService()
    {
        var jobScheduler = globalModule.getJobScheduler();
        jobScheduler.setParallelism( Group.THROUGHPUT_MONITOR, 1 );
        Duration throughputWindow = globalModule.getGlobalConfig().get( status_throughput_window );
        var throughputMonitorService = new ThroughputMonitorService( globalModule.getGlobalClock(), jobScheduler, throughputWindow, logProvider );
        globalModule.getGlobalLife().add( throughputMonitorService );
        globalModule.getGlobalDependencies().satisfyDependencies( throughputMonitorService );
    }

    @Override
    public DatabaseManager<ClusteredDatabaseContext> createDatabaseManager( GlobalModule globalModule )
    {
        var databaseManager = new CoreDatabaseManager( globalModule, this, catchupComponentsProvider::createDatabaseComponents,
                globalModule.getFileSystem(), globalModule.getPageCache(), logProvider, globalModule.getGlobalConfig(), clusterStateLayout );

        globalModule.getGlobalLife().add( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependency( databaseManager );

        createDatabaseManagerDependentModules( databaseManager );
        return databaseManager;
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        var systemGraphComponents = globalModule.getSystemGraphComponents();
        var systemGraphComponent = new EnterpriseSystemGraphComponent( globalModule.getGlobalConfig() );
        systemGraphComponents.register( systemGraphComponent );
        return globalModule.getGlobalDependencies().satisfyDependency( SystemGraphInitializer.NO_OP );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        setSecurityProvider( makeEnterpriseSecurityModule( globalModule ) );
    }

    @Override
    public DatabaseStartupController getDatabaseStartupController()
    {
        return databaseStartAborter;
    }

    @Override
    public Lifecycle createWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DatabaseInfo databaseInfo )
    {
        return new EnterpriseNeoWebServer( managementService, globalDependencies, config, userLogProvider, databaseInfo );
    }

    private static ClusterStateMigrator createClusterStateMigrator( GlobalModule globalModule, ClusterStateLayout clusterStateLayout,
            ClusterStateStorageFactory storageFactory )
    {
        var clusterStateVersionStorage = storageFactory.createClusterStateVersionStorage();
        var fs = globalModule.getFileSystem();
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        return new ClusterStateMigrator( fs, clusterStateLayout, clusterStateVersionStorage, logProvider );
    }

    CoreDatabaseFactory coreDatabaseFactory()
    {
        return coreDatabaseFactory;
    }

    private RaftChannelPoolService buildRaftChannelPoolService( GlobalModule globalModule )
    {
        var clientChannelInitializer = buildClientChannelInitializer( globalModule.getLogService() );
        var bootstrapConfig = BootstrapConfiguration.clientConfig( globalConfig );
        return new RaftChannelPoolService( bootstrapConfig, globalModule.getJobScheduler(), logProvider, clientChannelInitializer );
    }

    private ClientChannelInitializer buildClientChannelInitializer( LogService logService )
    {
        var applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedRaftProtocols );
        var modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

        var protocolInstallerRepository = new ProtocolInstallerRepository<>(
                List.of( new RaftProtocolClientInstallerV2.Factory( pipelineBuilders.client(), logProvider ),
                        new RaftProtocolClientInstallerV3.Factory( pipelineBuilders.client(), logProvider ) ),
                ModifierProtocolInstaller.allClientInstallers );

        var handshakeTimeout = globalConfig.get( CausalClusteringSettings.handshake_timeout );

        var handshakeInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilders.client(), handshakeTimeout, logProvider, logService.getUserLogProvider() );

        return new ClientChannelInitializer( handshakeInitializer, pipelineBuilders.client(), handshakeTimeout, logProvider );
    }

    private CoreTopologyService createTopologyService( MemberId myIdentity, DatabaseManager<ClusteredDatabaseContext> databaseManager,
            DatabaseStateService databaseStateService )
    {
        DiscoveryMemberFactory discoveryMemberFactory = new DefaultDiscoveryMemberFactory( databaseManager, databaseStateService );
        DiscoveryModule discoveryModule = new DiscoveryModule( myIdentity, discoveryServiceFactory, discoveryMemberFactory, globalModule,
                sslPolicyLoader );
        return discoveryModule.topologyService();
    }

    @Override
    public void bootstrapFabricServices()
    {
        fabricServicesBootstrap.bootstrapServices();
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var kernelDatabaseManagementService = super.createBoltDatabaseManagementServiceProvider(dependencies, managementService, monitors, clock, logService);
        return fabricServicesBootstrap.createBoltDatabaseManagementServiceProvider( kernelDatabaseManagementService, managementService, monitors, clock );
    }
}
