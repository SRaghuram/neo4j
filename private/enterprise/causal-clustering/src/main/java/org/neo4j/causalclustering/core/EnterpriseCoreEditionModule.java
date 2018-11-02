/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusterStateException;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule;
import org.neo4j.causalclustering.core.state.machines.id.FreeIdFilteredIdGeneratorFactory;
import org.neo4j.causalclustering.diagnostics.CoreMonitor;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import org.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
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
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
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
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.DefaultEditionModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
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
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_messages_log_path;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule extends DefaultEditionModule
{
    private final ConsensusModule consensusModule;
    private final ReplicationModule replicationModule;
    private final CoreTopologyService topologyService;
    protected final LogProvider logProvider;
    protected final Config config;
    private final Supplier<Stream<Pair<AdvertisedSocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;
    private final CoreServerModule coreServerModule;
    private final CoreStateMachinesModule coreStateMachinesModule;

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

    public EnterpriseCoreEditionModule( final PlatformModule platformModule,
            final DiscoveryServiceFactory discoveryServiceFactory )
    {
        final Dependencies dependencies = platformModule.dependencies;
        config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final DatabaseLayout databaseLayout = platformModule.storeLayout.databaseLayout( config.get( GraphDatabaseSettings.active_database ) );
        final LifeSupport life = platformModule.life;

        CoreMonitor.register( logging.getInternalLogProvider(), logging.getUserLogProvider(), platformModule.monitors );

        final File dataDir = config.get( GraphDatabaseSettings.data_directory );
        final ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, databaseLayout.databaseDirectory(), false );
        try
        {
            clusterStateDirectory.initialize( fileSystem );
        }
        catch ( ClusterStateException e )
        {
            throw new RuntimeException( e );
        }
        dependencies.satisfyDependency( clusterStateDirectory );

        AvailabilityGuard globalGuard = getGlobalAvailabilityGuard( platformModule.clock, logging, platformModule.config );
        threadToTransactionBridge = dependencies.satisfyDependency( new ThreadToStatementContextBridge( globalGuard ) );

        logProvider = logging.getInternalLogProvider();
        final Supplier<DatabaseHealth> databaseHealthSupplier =
                () -> platformModule.dataSourceManager.getDataSource().getDependencyResolver().resolveDependency( DatabaseHealth.class );

        watcherServiceFactory = directory -> createFileSystemWatcherService( fileSystem, directory, logging,
                platformModule.jobScheduler, config, fileWatcherFileNameFilter() );
        dependencies.satisfyDependencies( watcherServiceFactory );
        LogFiles logFiles = buildLocalDatabaseLogFiles( platformModule, fileSystem, databaseLayout );
        LocalDatabase localDatabase = new LocalDatabase( databaseLayout,
                new StoreFiles( fileSystem, platformModule.pageCache ),
                logFiles,
                platformModule.dataSourceManager,
                databaseHealthSupplier,
                globalGuard,
                logProvider );

        IdentityModule identityModule = new IdentityModule( platformModule, clusterStateDirectory.get() );

        ClusteringModule clusteringModule = getClusteringModule( platformModule, discoveryServiceFactory,
                clusterStateDirectory, identityModule, dependencies, databaseLayout );

        // We need to satisfy the dependency here to keep users of it, such as BoltKernelExtension, happy.
        dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );

        PipelineWrapper clientPipelineWrapper = pipelineWrapperFactory().forClient( config, dependencies, logProvider, CausalClusteringSettings.ssl_policy );
        PipelineWrapper serverPipelineWrapper = pipelineWrapperFactory().forServer( config, dependencies, logProvider, CausalClusteringSettings.ssl_policy );
        PipelineWrapper backupServerPipelineWrapper = pipelineWrapperFactory().forServer( config, dependencies, logProvider, OnlineBackupSettings.ssl_policy );

        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( clientPipelineWrapper );
        NettyPipelineBuilderFactory serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( serverPipelineWrapper );
        NettyPipelineBuilderFactory backupServerPipelineBuilderFactory = new NettyPipelineBuilderFactory( backupServerPipelineWrapper );

        topologyService = clusteringModule.topologyService();

        long logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedRaftProtocols = supportedProtocolCreator.createSupportedRaftProtocol();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), supportedRaftProtocols );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                asList( new RaftProtocolClientInstallerV2.Factory( clientPipelineBuilderFactory, logProvider ),
                        new RaftProtocolClientInstallerV1.Factory( clientPipelineBuilderFactory,
                                logProvider ) ),
                        ModifierProtocolInstaller.allClientInstallers );

        Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, clientPipelineBuilderFactory, handshakeTimeout, logProvider, platformModule.logging.getUserLogProvider() );
        final SenderService raftSender = new SenderService( channelInitializer, logProvider );
        life.add( raftSender );
        this.clientInstalledProtocols = raftSender::installedProtocols;

        final MessageLogger<MemberId> messageLogger = createMessageLogger( config, life, identityModule.myself() );

        RaftOutbound raftOutbound = new RaftOutbound( topologyService, raftSender, clusteringModule.clusterIdentity(),
                logProvider, logThresholdMillis );
        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>( raftOutbound,
                identityModule.myself(), messageLogger );

        consensusModule = new ConsensusModule( identityModule.myself(), platformModule, loggingOutbound,
                clusterStateDirectory.get(), topologyService );

        dependencies.satisfyDependency( consensusModule.raftMachine() );

        replicationModule = new ReplicationModule( consensusModule.raftMachine(), identityModule.myself(), platformModule, config, loggingOutbound,
                clusterStateDirectory.get(), fileSystem, logProvider, globalGuard, localDatabase );

        coreStateMachinesModule = new CoreStateMachinesModule( identityModule.myself(),
                platformModule, clusterStateDirectory.get(), config, replicationModule.getReplicator(),
                consensusModule.raftMachine(), dependencies, localDatabase );

        idContextFactory = IdContextFactoryBuilder.of( coreStateMachinesModule.idTypeConfigurationProvider, platformModule.jobScheduler )
                .withIdGenerationFactoryProvider( ignored -> coreStateMachinesModule.idGeneratorFactory )
                .withFactoryWrapper( generator -> new FreeIdFilteredIdGeneratorFactory( generator, coreStateMachinesModule.freeIdCondition ) ).build();

        // TODO: this is broken, coreStateMachinesModule.tokenHolders should be supplier, somehow...
        this.tokenHoldersProvider = databaseName -> coreStateMachinesModule.tokenHolders;
        this.locksSupplier = coreStateMachinesModule.locksSupplier;
        this.commitProcessFactory = coreStateMachinesModule.commitProcessFactory;
        this.accessCapability = new LeaderCanWrite( consensusModule.raftMachine() );

        InstalledProtocolHandler serverInstalledProtocolHandler = new InstalledProtocolHandler();

        this.coreServerModule = new CoreServerModule( identityModule, platformModule, consensusModule, coreStateMachinesModule, clusteringModule,
                replicationModule, localDatabase, databaseHealthSupplier, clusterStateDirectory.get(), clientPipelineBuilderFactory,
                serverPipelineBuilderFactory, backupServerPipelineBuilderFactory, serverInstalledProtocolHandler );

        TypicallyConnectToRandomReadReplicaStrategy defaultStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        defaultStrategy.inject( topologyService, config, logProvider, identityModule.myself() );
        UpstreamDatabaseStrategySelector catchupStrategySelector =
                createUpstreamDatabaseStrategySelector( identityModule.myself(), config, logProvider, topologyService, defaultStrategy );

        CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider =
                new CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider( consensusModule.raftMachine(), topologyService,
                        catchupStrategySelector );
        RaftServerModule.createAndStart( platformModule, consensusModule, identityModule, coreServerModule, localDatabase, serverPipelineBuilderFactory,
                messageLogger, catchupAddressProvider, supportedRaftProtocols, supportedModifierProtocols, serverInstalledProtocolHandler );
        serverInstalledProtocols = serverInstalledProtocolHandler::installedProtocols;

        editionInvariants( platformModule, dependencies, config, logging, life );

        life.add( coreServerModule.membershipWaiterLifecycle );
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

    private LogFiles buildLocalDatabaseLogFiles( PlatformModule platformModule, FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( databaseLayout, fileSystem, platformModule.pageCache ).withConfig( config ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected ClusteringModule getClusteringModule( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory,
            ClusterStateDirectory clusterStateDirectory, IdentityModule identityModule, Dependencies dependencies, DatabaseLayout databaseLayout )
    {
        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(),
                platformModule, clusterStateDirectory.get(), databaseLayout );
    }

    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new VoidPipelineWrapperFactory();
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any(
                fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME ),
                filename -> filename.startsWith( StoreUtil.TEMP_COPY_DIRECTORY_NAME ),
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

    private void editionInvariants( PlatformModule platformModule, Dependencies dependencies, Config config,
            LogService logging, LifeSupport life )
    {
        statementLocksFactoryProvider = locks -> new StatementLocksFactorySelector( locks, config, logging ).select();

        dependencies.satisfyDependency(
                createKernelData( platformModule.fileSystem, platformModule.pageCache, platformModule.storeLayout.storeDirectory(),
                        config, platformModule.dataSourceManager, life ) );

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

    private static KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config,
            DataSourceManager dataSourceManager, LifeSupport life )
    {
        KernelData kernelData = new KernelData( fileSystem, pageCache, storeDir, config, dataSourceManager );
        return life.add( kernelData );
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

    public void disableCatchupServer() throws Throwable
    {
        coreServerModule.catchupServer().disable();
    }
}
