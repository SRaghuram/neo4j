/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.ResolutionResolverFactory;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.member.DefaultDiscoveryMemberFactory;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitorService;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.ClusteredDbmsReconcilerModule;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.EnterpriseSystemGraphComponent;
import com.neo4j.dbms.SystemDbOnlyReplicatedDatabaseEventService;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.dbms.procedures.ClusteredDatabaseStateProcedure;
import com.neo4j.enterprise.edition.AbstractEnterpriseEditionModule;
import com.neo4j.fabric.bootstrap.EnterpriseFabricServicesBootstrap;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;
import com.neo4j.procedure.enterprise.builtin.SettingsWhitelist;
import com.neo4j.server.enterprise.EnterpriseNeoWebServer;

import java.io.File;
import java.time.Duration;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
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
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static java.util.UUID.randomUUID;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Read Replica edition.
 */
public class ReadReplicaEditionModule extends ClusteringEditionModule implements AbstractEnterpriseEditionModule
{
    protected final LogProvider logProvider;
    private final Config globalConfig;
    private final GlobalModule globalModule;

    private final MemberId myIdentity;
    private final JobScheduler jobScheduler;
    private final PanicService panicService;
    private final CatchupComponentsProvider catchupComponentsProvider;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final SslPolicyLoader sslPolicyLoader;

    private TopologyService topologyService;
    private ReadReplicaDatabaseFactory readReplicaDatabaseFactory;
    private ClusteredDbmsReconcilerModule reconcilerModule;
    private DatabaseStartAborter databaseStartAborter;
    private final ClusterStateStorageFactory storageFactory;
    private final ClusterStateLayout clusterStateLayout;
    private final EnterpriseFabricServicesBootstrap fabricServicesBootstrap;
    private AuthManager inClusterAuthManager;

    public ReadReplicaEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        this( globalModule, discoveryServiceFactory, new MemberId( randomUUID() ) );
    }

    public ReadReplicaEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory, final MemberId myIdentity )
    {
        super( globalModule );

        this.globalModule = globalModule;
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.myIdentity = myIdentity;
        LogService logService = globalModule.getLogService();
        this.globalConfig = globalModule.getGlobalConfig();
        logProvider = logService.getInternalLogProvider();
        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myIdentity ) );

        jobScheduler = globalModule.getJobScheduler();
        jobScheduler.setTopLevelGroupName( "ReadReplica " + myIdentity );

        Dependencies globalDependencies = globalModule.getGlobalDependencies();

        SettingsWhitelist settingsWhiteList = new SettingsWhitelist( globalConfig );
        globalDependencies.satisfyDependency( settingsWhiteList );

        panicService = new PanicService( jobScheduler, logService );
        // used in tests
        globalDependencies.satisfyDependencies( panicService );

        watcherServiceFactory = layout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService, fileWatcherFileNameFilter() );

        sslPolicyLoader = SslPolicyLoader.create( globalConfig, logProvider );
        globalDependencies.satisfyDependency( sslPolicyLoader );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( sslPolicyLoader );
        catchupComponentsProvider = new CatchupComponentsProvider( globalModule, pipelineBuilders );

        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();

        final File dataDir = globalConfig.get( GraphDatabaseSettings.data_directory ).toFile();
        clusterStateLayout = ClusterStateLayout.of( dataDir );
        storageFactory = new ClusterStateStorageFactory( fileSystem, clusterStateLayout, logProvider, globalConfig,
                globalModule.getOtherMemoryPool().getPoolMemoryTracker() );

        addThroughputMonitorService();

        satisfyEnterpriseOnlyDependencies( this.globalModule );

        editionInvariants( globalModule, globalDependencies );

        fabricServicesBootstrap = new EnterpriseFabricServicesBootstrap.ReadReplica( globalModule.getGlobalLife(), globalDependencies, logService );
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
    public QueryEngineProvider getQueryEngineProvider()
    {
        // Clustering is enterprise only
        return new EnterpriseCypherEngineProvider();
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new ReadReplicaRoleProcedure( databaseManager ) );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService, databaseManager.databaseIdRepository() ) );
        globalProcedures.register( new ClusteredDatabaseStateProcedure( databaseManager.databaseIdRepository(), topologyService,
                reconcilerModule.databaseStateService() ) );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        return new ReadReplicaRoutingProcedureInstaller( databaseManager, portRegister, config, logProvider );
    }

    @Override
    public EditionDatabaseComponents createDatabaseComponents( NamedDatabaseId namedDatabaseId )
    {
        return new ReadReplicaDatabaseComponents( globalModule, this, namedDatabaseId );
    }

    @Override
    public DatabaseManager<ClusteredDatabaseContext> createDatabaseManager( GlobalModule globalModule )
    {
        var databaseManager = new ReadReplicaDatabaseManager( globalModule, this, catchupComponentsProvider::createDatabaseComponents,
                globalModule.getFileSystem(), globalModule.getPageCache(), logProvider, globalConfig, clusterStateLayout );

        globalModule.getGlobalLife().add( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependency( databaseManager );

        createDatabaseManagerDependentModules( databaseManager );
        return databaseManager;
    }

    private void createDatabaseManagerDependentModules( ReadReplicaDatabaseManager databaseManager )
    {
        var databaseEventService = new SystemDbOnlyReplicatedDatabaseEventService( logProvider );
        var globalLife = globalModule.getGlobalLife();
        var globalLogService = globalModule.getLogService();
        var fileSystem = globalModule.getFileSystem();
        var dependencies = globalModule.getGlobalDependencies();

        dependencies.satisfyDependency( reconciledTxTracker );
        dependencies.satisfyDependencies( databaseEventService );

        Supplier<GraphDatabaseService> systemDbSupplier = () -> databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID )
                .orElseThrow()
                .databaseFacade();
        var dbmsModel = new ClusterSystemGraphDbmsModel( systemDbSupplier );
        reconcilerModule = new ClusteredDbmsReconcilerModule( globalModule, databaseManager,
                databaseEventService, storageFactory, reconciledTxTracker, dbmsModel );

        topologyService = createTopologyService( databaseManager, reconcilerModule.databaseStateService(), globalLogService );
        reconcilerModule.registerDatabaseStateChangedListener( topologyService );
        globalLife.add( dependencies.satisfyDependency( topologyService ) );

        int maxChunkSize = globalConfig.get( CausalClusteringSettings.store_copy_chunk_size );
        var catchupServerHandler = new MultiDatabaseCatchupServerHandler( databaseManager, fileSystem, maxChunkSize, logProvider );
        var installedProtocolsHandler = new InstalledProtocolHandler();

        var catchupServer = catchupComponentsProvider.createCatchupServer( installedProtocolsHandler, catchupServerHandler );
        var backupServerOptional = catchupComponentsProvider.createBackupServer( installedProtocolsHandler, catchupServerHandler );

        var catchupComponentsRepository = new CatchupComponentsRepository( databaseManager );
        var catchupClientFactory = catchupComponentsProvider.catchupClientFactory();

        globalLife.add( catchupServer );
        backupServerOptional.ifPresent( globalLife::add );
        //Reconciler module must start last, as it starting starts actual databases, which depend on all of the above components at runtime.
        globalLife.add( reconcilerModule );

        databaseStartAborter = new DatabaseStartAborter( globalModule.getGlobalAvailabilityGuard(), dbmsModel, globalModule.getGlobalClock(),
                Duration.ofSeconds( 5 ) );

        // TODO: Health should be created per-db in the factory. What about other things here?
        readReplicaDatabaseFactory = new ReadReplicaDatabaseFactory( globalConfig, globalModule.getGlobalClock(), jobScheduler,
                topologyService, myIdentity, catchupComponentsRepository, catchupClientFactory, databaseEventService, storageFactory, panicService,
                databaseStartAborter, globalModule.getTracers().getPageCacheTracer() );
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
        setSecurityProvider( makeEnterpriseSecurityModule( globalModule, globalProcedures ) );
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

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    private TopologyService createTopologyService( DatabaseManager<ClusteredDatabaseContext> databaseManager, DatabaseStateService databaseStateService,
            LogService logService )
    {
        DiscoveryMemberFactory discoveryMemberFactory = new DefaultDiscoveryMemberFactory( databaseManager, databaseStateService );
        RemoteMembersResolver hostnameResolver = ResolutionResolverFactory.chooseResolver( globalConfig, logService );
        return discoveryServiceFactory.readReplicaTopologyService( globalConfig, logProvider, jobScheduler, myIdentity, hostnameResolver,
                sslPolicyLoader, discoveryMemberFactory, globalModule.getGlobalClock() );
    }

    ReadReplicaDatabaseFactory readReplicaDatabaseFactory()
    {
        return readReplicaDatabaseFactory;
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
