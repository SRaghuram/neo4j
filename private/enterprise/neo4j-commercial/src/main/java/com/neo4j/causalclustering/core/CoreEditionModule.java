/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftGroupFactory;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.ClusterStateMigrator;
import com.neo4j.causalclustering.core.state.DiscoveryModule;
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
import com.neo4j.causalclustering.routing.load_balancing.LeaderLocatorForDatabase;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.commercial.edition.AbstractCommercialEditionModule;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusteredDbmsReconcilerModule;
import com.neo4j.dbms.ReplicatedTransactionEventListeners;
import com.neo4j.dbms.SystemDbOnlyReplicatedTransactionEventListeners;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.procedure.commercial.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.commercial.builtin.EnterpriseBuiltInProcedures;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.systemgraph.CommercialSystemGraphInitializer;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.cypher.internal.javacompat.EnterpriseCypherEngineProvider;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.RecoveryFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.CompositeDatabaseHealth;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.ssl.config.SslPolicyLoader;

import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.recovery.Recovery.recoveryFacade;

/**
 * This implementation of {@link AbstractEditionModule} creates the service instances
 * which are specific to the Core members of a causal cluster.
 */
public class CoreEditionModule extends ClusteringEditionModule implements AbstractCommercialEditionModule
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

    private final Map<DatabaseId,DatabaseInitializer> databaseInitializerMap = new HashMap<>();
    private final CompositeDatabaseHealth globalHealth;
    private final LogProvider logProvider;
    private final Config globalConfig;
    private final GlobalModule globalModule;
    private final CommercialTemporaryDatabaseFactory temporaryDatabaseFactory;
    private final RaftSender raftSender;

    private CoreDatabaseFactory coreDatabaseFactory;
    private CoreTopologyService topologyService;

    public CoreEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        super( globalModule );

        final Dependencies globalDependencies = globalModule.getGlobalDependencies();
        final LogService logService = globalModule.getLogService();
        final LifeSupport globalLife = globalModule.getGlobalLife();

        this.globalModule = globalModule;
        this.globalConfig = globalModule.getGlobalConfig();
        this.globalHealth = globalModule.getGlobalHealthService();
        this.logProvider = logService.getInternalLogProvider();

        RaftMonitor.register( logService, globalModule.getGlobalMonitors() );

        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();

        final File dataDir = globalConfig.get( GraphDatabaseSettings.data_directory ).toFile();
        clusterStateLayout = ClusterStateLayout.of( dataDir );
        globalDependencies.satisfyDependency( clusterStateLayout );
        storageFactory = new ClusterStateStorageFactory( fileSystem, clusterStateLayout, logProvider, globalConfig );

        // migration needs to happen as early as possible in the lifecycle
        var clusterStateMigrator = createClusterStateMigrator( globalModule, clusterStateLayout, storageFactory );
        globalLife.add( clusterStateMigrator );

        temporaryDatabaseFactory = new CommercialTemporaryDatabaseFactory( globalModule.getPageCache() );

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

        satisfyCommercialOnlyDependencies( this.globalModule );

        editionInvariants( globalModule, globalDependencies, globalConfig, globalLife );
    }

    private void createCoreServers( LifeSupport life, DatabaseManager<?> databaseManager, FileSystemAbstraction fileSystem )
    {
        CatchupServerHandler catchupServerHandler = new MultiDatabaseCatchupServerHandler( databaseManager, fileSystem, logProvider );
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
    public EditionDatabaseComponents createDatabaseComponents( DatabaseId databaseId )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseIdRepository databaseIdRepository ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService ) );
        globalProcedures.register( new CoreRoleProcedure( identityModule, topologyService, databaseIdRepository ) );
        globalProcedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, serverInstalledProtocols ) );
        // TODO: Figure out how the replication benchmark procedure should work.
//        globalProcedures.registerComponent( Replicator.class, x -> replicationModule.getReplicator(), false );
//        globalProcedures.registerProcedure( ReplicationBenchmarkProcedure.class );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        LeaderLocatorForDatabase leaderLocatorForDatabase = databaseId -> databaseManager
                .getDatabaseContext( databaseId )
                .map( DatabaseContext::dependencies )
                .map( dep -> dep.resolveDependency( LeaderLocator.class ) );

        LeaderService leaderService = new DefaultLeaderService( leaderLocatorForDatabase, topologyService );

        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
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

    private void createDatabaseManagerDependentModules( final CoreDatabaseManager databaseManager, ReplicatedTransactionEventListeners txEventService )
    {
        final LifeSupport globalLife = globalModule.getGlobalLife();
        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        MemberId myIdentity = identityModule.myself();

        DiscoveryMemberFactory discoveryMemberFactory = new DefaultDiscoveryMemberFactory( databaseManager );
        DiscoveryModule discoveryModule = new DiscoveryModule( myIdentity, discoveryServiceFactory, discoveryMemberFactory, globalModule, sslPolicyLoader );

        topologyService = discoveryModule.topologyService();

        final RaftMessageLogger<MemberId> raftLogger = createRaftLogger( globalModule, myIdentity );

        RaftMessageDispatcher raftMessageDispatcher = new RaftMessageDispatcher( logProvider, globalModule.getGlobalClock() );

        RaftGroupFactory raftGroupFactory = new RaftGroupFactory( myIdentity, globalModule, clusterStateLayout, topologyService, storageFactory );

        RecoveryFacade recoveryFacade = recoveryFacade( globalModule.getFileSystem(), globalModule.getPageCache(), globalConfig,
                globalModule.getStorageEngineFactory() );

        this.coreDatabaseFactory = new CoreDatabaseFactory( globalModule, panicService, databaseManager, topologyService, storageFactory,
                temporaryDatabaseFactory, databaseInitializerMap, myIdentity, raftGroupFactory, raftMessageDispatcher, catchupComponentsProvider,
                recoveryFacade, raftLogger, raftSender, txEventService );

        RaftServerFactory raftServerFactory = new RaftServerFactory( globalModule, identityModule, pipelineBuilders.server(), raftLogger,
                supportedRaftProtocols, supportedModifierProtocols );

        Server raftServer = raftServerFactory.createRaftServer( raftMessageDispatcher, serverInstalledProtocolHandler );
        globalModule.getGlobalDependencies().satisfyDependencies( raftServer ); // resolved in tests
        globalLife.add( raftServer );

        // must start last and stop first, since it handles external requests
        createCoreServers( globalLife, databaseManager, fileSystem );
    }

    // TODO extract common
    @Override
    public DatabaseManager<?> createDatabaseManager( GlobalModule globalModule )
    {
        var internalOperator = new ClusterInternalDbmsOperator();

        //TODO: Pass internal operator to database manager so it can pass to factories
        var databaseManager = new CoreDatabaseManager( globalModule, this, catchupComponentsProvider::createDatabaseComponents,
                globalModule.getFileSystem(), globalModule.getPageCache(), logProvider, globalModule.getGlobalConfig(), clusterStateLayout );

        ReplicatedTransactionEventListeners txEventListeners = new SystemDbOnlyReplicatedTransactionEventListeners();
        createDatabaseManagerDependentModules( databaseManager, txEventListeners );

        var dependencies = globalModule.getGlobalDependencies();
        dependencies.satisfyDependencies( txEventListeners );

        globalModule.getGlobalLife().add( databaseManager );
        dependencies.satisfyDependency( databaseManager );

        var reconcilerModule = new ClusteredDbmsReconcilerModule( globalModule, databaseManager, txEventListeners, internalOperator,
                storageFactory, reconciledTxTracker, panicService );
        globalModule.getGlobalLife().add( reconcilerModule );
        dependencies.satisfyDependency( reconciledTxTracker );

        return databaseManager;
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        SystemGraphInitializer initializer =
                CommunityEditionModule.tryResolveOrCreate( SystemGraphInitializer.class, globalModule.getExternalDependencyResolver(),
                        () -> new CommercialSystemGraphInitializer( databaseManager, globalModule.getGlobalConfig() ) );
        databaseInitializerMap.put( SYSTEM_DATABASE_ID, db ->
        {
            try
            {
                initializer.initializeSystemGraph( db );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );
        return globalModule.getGlobalDependencies().satisfyDependency( initializer );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        SecurityProvider securityProvider;
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            CommercialSecurityModule securityModule = (CommercialSecurityModule) setupSecurityModule( globalModule,
                    globalModule.getLogService().getUserLog( CoreEditionModule.class ), globalProcedures, "commercial-security-module" );
            securityModule.getDatabaseInitializer().ifPresent( dbInit -> databaseInitializerMap.put( SYSTEM_DATABASE_ID, dbInit ) );
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = CommercialNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
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
                List.of( new RaftProtocolClientInstallerV2.Factory( pipelineBuilders.client(), logProvider ) ),
                ModifierProtocolInstaller.allClientInstallers );

        var handshakeTimeout = globalConfig.get( CausalClusteringSettings.handshake_timeout );

        var handshakeInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilders.client(), handshakeTimeout, logProvider, logService.getUserLogProvider() );

        return new ClientChannelInitializer( handshakeInitializer, pipelineBuilders.client(), handshakeTimeout, logProvider );
    }
}
