/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.catchup.RegularCatchupServerHandler;
import org.neo4j.causalclustering.common.DefaultDatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.common.PipelineBuilders;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.CoreStateService;
import org.neo4j.causalclustering.core.state.CoreStateStorageService;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.diagnostics.CoreMonitor;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import org.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.BetterMessageLogger;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.logging.NullMessageLogger;
import org.neo4j.causalclustering.messaging.LoggingOutbound;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.causalclustering.messaging.RaftOutbound;
import org.neo4j.causalclustering.messaging.SenderService;
import org.neo4j.causalclustering.net.InstalledProtocolHandler;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingPluginLoader;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForMultiDC;
import org.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForSingleDC;
import org.neo4j.causalclustering.routing.load_balancing.procedure.LegacyGetServersProcedure;
import org.neo4j.causalclustering.routing.multi_cluster.procedure.GetRoutersForAllDatabasesProcedure;
import org.neo4j.causalclustering.routing.multi_cluster.procedure.GetRoutersForDatabaseProcedure;
import org.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.causalclustering.upstream.strategies.TypicallyConnectToRandomReadReplicaStrategy;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_messages_log_path;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.LAST_FLUSHED;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule extends AbstractEditionModule
{
    private final ConsensusModule consensusModule;
    private final ReplicationModule replicationModule;
    private final CoreTopologyService topologyService;
    protected final LogProvider logProvider;
    protected final Config config;
    private final Supplier<Stream<Pair<AdvertisedSocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;
    private final CoreServerModule coreServerModule;
    private final CoreStateService coreStateService;
    protected final DefaultDatabaseService<CoreLocalDatabase> databaseService;
    protected final Map<String,DatabaseInitializer> databaseInitializers = new HashMap<>();
    //TODO: Find a way to be more generic about this to help with 4.0 plans
    private final String activeDatabaseName;
    private final PlatformModule platformModule;

    public enum RaftLogImplementation
    {
        IN_MEMORY, SEGMENTED
    }

    private LoadBalancingProcessor getLoadBalancingProcessor()
    {
        try
        {
            return LoadBalancingPluginLoader.load( topologyService, consensusModule.raftMachine(), logProvider, config );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        //noinspection deprecation
        procedures.register( new LegacyGetServersProcedure( topologyService, consensusModule.raftMachine(), config, logProvider ) );

        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            procedures.register( new GetServersProcedureForMultiDC( getLoadBalancingProcessor() ) );
        }
        else
        {
            procedures.register( new GetServersProcedureForSingleDC( topologyService, consensusModule.raftMachine(),
                    config, logProvider ) );
        }

        procedures.register( new GetRoutersForAllDatabasesProcedure( topologyService, config ) );
        procedures.register( new GetRoutersForDatabaseProcedure( topologyService, config ) );
        procedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
        procedures.register( new CoreRoleProcedure( consensusModule.raftMachine() ) );
        procedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, serverInstalledProtocols ) );
        procedures.registerComponent( Replicator.class, x -> replicationModule.getReplicator(), false );
        procedures.registerProcedure( ReplicationBenchmarkProcedure.class );
    }

    public EnterpriseCoreEditionModule( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        final Dependencies dependencies = platformModule.dependencies;
        final LogService logging = platformModule.logService;
        final LifeSupport life = platformModule.life;

        this.platformModule = platformModule;
        config = platformModule.config;
        logProvider = logging.getInternalLogProvider();

        CoreMonitor.register( logProvider, logging.getUserLogProvider(), platformModule.monitors );

        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        this.activeDatabaseName = config.get( GraphDatabaseSettings.active_database );
        final DatabaseLayout activeDatabaseLayout = platformModule.storeLayout.databaseLayout( activeDatabaseName );

        final File dataDir = config.get( GraphDatabaseSettings.data_directory );
        /* Database directory is passed here to support migration from earlier versions of cluster state, which were stored *inside* the database directory */
        final ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( fileSystem, dataDir, activeDatabaseLayout.databaseDirectory(), false );
        clusterStateDirectory.initialize();
        dependencies.satisfyDependency( clusterStateDirectory );
        CoreStateStorageService storage = new CoreStateStorageService( fileSystem, clusterStateDirectory, platformModule.life, logProvider, config );
        storage.migrateIfNecessary( activeDatabaseName );

        AvailabilityGuard globalGuard = getGlobalAvailabilityGuard( platformModule.clock, logging, platformModule.config );
        threadToTransactionBridge = dependencies.satisfyDependency( new ThreadToStatementContextBridge( globalGuard ) );
        watcherServiceFactory = layout ->
                createDatabaseFileSystemWatcher( platformModule.fileSystemWatcher.getFileWatcher(), layout, logging, fileWatcherFileNameFilter() );

        IdentityModule identityModule = new IdentityModule( platformModule, clusterStateDirectory.get() );

        //Build local databases object
        final Supplier<DatabaseManager> databaseManagerSupplier = () -> platformModule.dependencies.resolveDependency( DatabaseManager.class );
        final Supplier<DatabaseHealth> databaseHealthSupplier =
                () -> databaseManagerSupplier.get().getDatabaseFacade( DEFAULT_DATABASE_NAME )
                        .map( GraphDatabaseFacade::getDependencyResolver )
                        .map( resolver -> resolver.resolveDependency( DatabaseHealth.class ) )
                        .orElseThrow( () -> new IllegalStateException( "Default database not found." ) );

        this.databaseService = createDatabasesService( databaseHealthSupplier, fileSystem, globalAvailabilityGuard, platformModule,
                databaseManagerSupplier, logProvider, config );
        dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );

        ClusteringModule clusteringModule = getClusteringModule( platformModule, discoveryServiceFactory, storage, identityModule, dependencies );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, logProvider, config, dependencies );

        topologyService = clusteringModule.topologyService();

        long logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
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

        Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilders.client(), handshakeTimeout, logProvider, platformModule.logService.getUserLogProvider() );
        final SenderService raftSender = new SenderService( channelInitializer, platformModule.jobScheduler, logProvider );
        life.add( raftSender );
        this.clientInstalledProtocols = raftSender::installedProtocols;

        final MessageLogger<MemberId> messageLogger = createMessageLogger( config, life, identityModule.myself() );

        RaftOutbound raftOutbound = new RaftOutbound( topologyService, raftSender, clusteringModule.clusterIdentity(), logProvider, logThresholdMillis );
        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>( raftOutbound, identityModule.myself(), messageLogger );

        consensusModule = new ConsensusModule( identityModule.myself(), platformModule,
                loggingOutbound, clusterStateDirectory.get(), topologyService, storage, activeDatabaseName );

        dependencies.satisfyDependency( consensusModule.raftMachine() );
        replicationModule = new ReplicationModule( consensusModule.raftMachine(), identityModule.myself(), platformModule, config,
                loggingOutbound, storage, logProvider, globalGuard, databaseService );

        StateStorage<Long> lastFlushedStorage = storage.stateStorage( LAST_FLUSHED );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStorage::getInitialState );

        coreStateService = new CoreStateService( identityModule.myself(), platformModule, storage, config,
                consensusModule.raftMachine(), databaseService, replicationModule, lastFlushedStorage );

        this.accessCapability = new LeaderCanWrite( consensusModule.raftMachine() );

        InstalledProtocolHandler serverInstalledProtocolHandler = new InstalledProtocolHandler();

        CatchupHandlerFactory handlerFactory = snapshotService -> getHandlerFactory( platformModule, fileSystem, snapshotService );

        this.coreServerModule = new CoreServerModule( identityModule, platformModule, consensusModule, coreStateService, clusteringModule,
                replicationModule, databaseService, databaseHealthSupplier, pipelineBuilders, serverInstalledProtocolHandler,
                handlerFactory, activeDatabaseName );

        TypicallyConnectToRandomReadReplicaStrategy defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, config, logProvider, identityModule.myself() );
        UpstreamDatabaseStrategySelector catchupStrategySelector =
                createUpstreamDatabaseStrategySelector( identityModule.myself(), config, logProvider, topologyService, defaultStrategy );

        CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider =
                new CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider( consensusModule.raftMachine(), topologyService,
                        catchupStrategySelector );
        RaftServerModule.createAndStart( platformModule, consensusModule, identityModule, coreServerModule, databaseService, pipelineBuilders.server(),
                messageLogger, catchupAddressProvider, supportedRaftProtocols, supportedModifierProtocols, serverInstalledProtocolHandler, activeDatabaseName );
        serverInstalledProtocols = serverInstalledProtocolHandler::installedProtocols;

        editionInvariants( platformModule, dependencies, config, life );

        life.add( coreServerModule.membershipWaiterLifecycle );
    }

    @Override
    public EditionDatabaseContext createDatabaseContext( String databaseName )
    {
        return new CoreDatabaseContext( platformModule, this, databaseName );
    }

    private DefaultDatabaseService<CoreLocalDatabase> createDatabasesService( Supplier<DatabaseHealth> databaseHealthSupplier,
            FileSystemAbstraction fileSystem, AvailabilityGuard availabilityGuard, PlatformModule platformModule,
            Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, Config config )
    {
        return new DefaultDatabaseService<>( CoreLocalDatabase::new, databaseManagerSupplier, platformModule.storeLayout,
                availabilityGuard, databaseHealthSupplier, fileSystem, platformModule.pageCache, platformModule.jobScheduler, logProvider, config );
    }

    protected CatchupServerHandler getHandlerFactory( PlatformModule platformModule,
            FileSystemAbstraction fileSystem, CoreSnapshotService snapshotService )
    {
        //TODO: Undo all the suppliers here when we fix the init order
        Supplier<LocalDatabase> localDatabase = () -> databaseService.get( activeDatabaseName ).orElseThrow( IllegalStateException::new );

        CheckPointerService checkPointerService =
                new CheckPointerService( () -> localDatabase.get().dependencies().resolveDependency( CheckPointer.class ),
                        platformModule.jobScheduler, Group.CHECKPOINT );

        return new RegularCatchupServerHandler( platformModule.monitors, logProvider, () -> localDatabase.get().storeId(),
                () -> localDatabase.get().dataSource(), databaseService::areAvailable, fileSystem, snapshotService, checkPointerService );
    }

    CoreStateService coreStateComponents()
    {
        return coreStateService;
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

    protected ClusteringModule getClusteringModule( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory,
            CoreStateStorageService storage, IdentityModule identityModule, Dependencies dependencies )
    {
        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(), platformModule, storage, databaseService, databaseInitializers );
    }

    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new VoidPipelineWrapperFactory();
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any(
                fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF )
        );
    }

    private static MessageLogger<MemberId> createMessageLogger( Config config, LifeSupport life, MemberId myself )
    {
        final MessageLogger<MemberId> messageLogger;
        if ( config.get( CausalClusteringSettings.raft_messages_log_enable ) )
        {
            File logFile = config.get( raft_messages_log_path );
            messageLogger = life.add( new BetterMessageLogger<>( myself, raftMessagesLog( logFile ), Clocks.systemClock() ) );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }
        return messageLogger;
    }

    private void editionInvariants( PlatformModule platformModule, Dependencies dependencies, Config config, LifeSupport life )
    {
        KernelData kernelData = createKernelData( platformModule.fileSystem, platformModule.pageCache, platformModule.storeLayout.storeDirectory(), config );
        dependencies.satisfyDependency( kernelData );
        life.add( kernelData );

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = new EnterpriseConstraintSemantics();

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );
    }

    public boolean isLeader()
    {
        return consensusModule.raftMachine().currentRole() == Role.LEADER;
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

    private static SchemaWriteGuard createSchemaWriteGuard()
    {
        return SchemaWriteGuard.ALLOW_ALL_WRITES;
    }

    private static KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config )
    {
        return new KernelData( fileSystem, pageCache, storeDir, config );
    }

    private static TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.createEnterpriseSecurityModule( this, platformModule, procedures );
    }

    void disableCatchupServer() throws Throwable
    {
        coreServerModule.catchupServer().disable();
    }

    @Override
    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        createDatabase( databaseManager, activeDatabaseName );
    }

    ConsensusModule consensusModule()
    {
        return consensusModule;
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
}
