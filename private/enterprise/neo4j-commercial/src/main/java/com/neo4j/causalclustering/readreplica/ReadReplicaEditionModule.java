/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.discovery.DefaultDiscoveryMember;
import com.neo4j.causalclustering.discovery.DiscoveryMember;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.ResolutionResolverFactory;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.error_handling.PanicEventHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.dbms.InternalOperator;
import com.neo4j.dbms.LocalOperator;
import com.neo4j.dbms.OperatorConnector;
import com.neo4j.dbms.OperatorState;
import com.neo4j.dbms.ReconcilingDatabaseOperator;
import com.neo4j.dbms.SystemOperator;
import com.neo4j.kernel.enterprise.api.security.provider.CommercialNoAuthSecurityProvider;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.CompositeDatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.procedure.commercial.builtin.EnterpriseBuiltInProcedures;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Commercial Read Replica edition.
 */
public class ReadReplicaEditionModule extends ClusteringEditionModule
{
    protected final LogProvider logProvider;
    private final Config globalConfig;
    private final CompositeDatabaseHealth globalHealth;
    private final GlobalModule globalModule;

    private final MemberId myIdentity;
    private final JobScheduler jobScheduler;
    private final PanicService panicService;
    private final CatchupComponentsProvider catchupComponentsProvider;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final SslPolicyLoader sslPolicyLoader;

    private TopologyService topologyService;
    private ReadReplicaDatabaseFactory readReplicaDatabaseFactory;

    public ReadReplicaEditionModule( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myIdentity )
    {
        this.globalModule = globalModule;
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.globalHealth = globalModule.getGlobalHealthService();
        this.myIdentity = myIdentity;
        LogService logService = globalModule.getLogService();
        this.globalConfig = globalModule.getGlobalConfig();
        logProvider = logService.getInternalLogProvider();
        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myIdentity ) );

        jobScheduler = globalModule.getJobScheduler();
        jobScheduler.setTopLevelGroupName( "ReadReplica " + myIdentity );

        Dependencies globalDependencies = globalModule.getGlobalDependencies();

        panicService = new PanicService( logService.getUserLogProvider() );
        // used in tests
        globalDependencies.satisfyDependencies( panicService );

        LifeSupport globalLife = globalModule.getGlobalLife();

        this.accessCapability = new ReadOnly();

        watcherServiceFactory = layout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), layout, logService, fileWatcherFileNameFilter() );

        sslPolicyLoader = SslPolicyLoader.create( globalConfig, logProvider );
        globalDependencies.satisfyDependency( sslPolicyLoader );

        PipelineBuilders pipelineBuilders = new PipelineBuilders( this::pipelineWrapperFactory, globalConfig, sslPolicyLoader );
        catchupComponentsProvider = new CatchupComponentsProvider( globalModule, pipelineBuilders );

        editionInvariants( globalModule, globalDependencies, globalConfig, globalLife );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new ReadReplicaRoleProcedure() );
        globalProcedures.register( new ClusterOverviewProcedure( topologyService ) );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        return new ReadReplicaRoutingProcedureInstaller( databaseManager, portRegister, config );
    }

    private void addPanicEventHandlers( PanicService panicService, LifeSupport life, Health globalHealth, Server catchupServer, Optional<Server> backupServer )
    {
        // order matters
        panicService.addPanicEventHandler( PanicEventHandlers.raiseAvailabilityGuardEventHandler( globalModule.getGlobalAvailabilityGuard() ) );
        panicService.addPanicEventHandler( PanicEventHandlers.dbHealthEventHandler( globalHealth ) );
        panicService.addPanicEventHandler( PanicEventHandlers.stopServerEventHandler( catchupServer ) );
        backupServer.ifPresent( server -> panicService.addPanicEventHandler( PanicEventHandlers.stopServerEventHandler( server ) ) );
        panicService.addPanicEventHandler( PanicEventHandlers.shutdownLifeCycle( life ) );
    }

    @Override
    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    @Override
    public EditionDatabaseComponents createDatabaseComponents( DatabaseId databaseId )
    {
        return new ReadReplicaDatabaseComponents( globalModule, this, databaseId );
    }

    @Override
    public DatabaseManager<?> createDatabaseManager( GlobalModule platform, Log log )
    {
        var databaseManager = new ReadReplicaDatabaseManager( platform, this, log, catchupComponentsProvider::createDatabaseComponents,
                platform.getFileSystem(), platform.getPageCache(), logProvider, globalConfig, globalHealth );
        createDatabaseManagerDependentModules( databaseManager );
        return databaseManager;
    }

    private void createDatabaseManagerDependentModules( ClusteredDatabaseManager databaseManager )
    {
        LifeSupport globalLife = globalModule.getGlobalLife();
        Dependencies dependencies = globalModule.getGlobalDependencies();
        LogService logService = globalModule.getLogService();

        topologyService = createTopologyService( databaseManager, logService );
        globalLife.add( dependencies.satisfyDependency( topologyService ) );

        CatchupHandlerFactory handlerFactory = () -> getHandlerFactory( globalModule.getFileSystem(), databaseManager );
        ReadReplicaServerModule serverModule = new ReadReplicaServerModule( databaseManager, catchupComponentsProvider, handlerFactory );

        // TODO: Most of this panic stuff isn't database manager dependent. Break out? Remove this catch-all method?
        addPanicEventHandlers( panicService, globalModule.getGlobalLife(), globalHealth, serverModule.catchupServer(), serverModule.backupServer() );

        globalLife.add( serverModule.catchupServer() ); // must start last and stop first, since it handles external requests
        serverModule.backupServer().ifPresent( globalLife::add );

        // TODO: Health should be created per-db in the factory. What about other things here?
        readReplicaDatabaseFactory = new ReadReplicaDatabaseFactory( globalConfig, globalModule.getGlobalClock(), jobScheduler, logService, topologyService,
                myIdentity, serverModule.catchupComponents(), globalModule.getTracers().getPageCursorTracerSupplier(), globalHealth,
                serverModule.catchupClient() );
    }

    @Override
    public void createDatabases( DatabaseManager<?> databaseManager, Config config ) throws DatabaseExistsException
    {
        var initialDatabases = new LinkedHashMap<DatabaseId,OperatorState>();

        initialDatabases.put( new DatabaseId( SYSTEM_DATABASE_NAME ), STOPPED );
        initialDatabases.put( new DatabaseId( config.get( default_database ) ), STOPPED );

        initialDatabases.keySet().forEach( databaseManager::createDatabase );

        setupDatabaseOperators( databaseManager, initialDatabases );
    }

    private void setupDatabaseOperators( DatabaseManager<?> databaseManager, Map<DatabaseId,OperatorState> initialDatabases )
    {
        var reconciler = new ReconcilingDatabaseOperator( databaseManager, initialDatabases );
        var connector = new OperatorConnector( reconciler );

        var localOperator = new LocalOperator( connector );
        var internalOperator = new InternalOperator( connector );
        var systemOperator = new SystemOperator( connector );

        globalModule.getGlobalDependencies().satisfyDependencies( internalOperator ); // for internal components
        globalModule.getGlobalDependencies().satisfyDependencies( localOperator ); // for admin procedures
    }

    private DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }

    private CatchupServerHandler getHandlerFactory( FileSystemAbstraction fileSystem, DatabaseManager<?> databaseManager )
    {
        return new MultiDatabaseCatchupServerHandler( databaseManager, logProvider, fileSystem );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        SecurityProvider securityProvider;
        if ( globalConfig.get( GraphDatabaseSettings.auth_enabled ) )
        {
            CommercialSecurityModule securityModule = (CommercialSecurityModule) setupSecurityModule( globalModule,
                    globalModule.getLogService().getUserLog( ReadReplicaEditionModule.class ), globalProcedures, "commercial-security-module" );
            globalModule.getGlobalLife().add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            securityProvider = CommercialNoAuthSecurityProvider.INSTANCE;
        }
        setSecurityProvider( securityProvider );
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    private TopologyService createTopologyService( DatabaseManager<?> databaseManager, LogService logService )
    {
        DiscoveryMember discoveryMember = new DefaultDiscoveryMember( myIdentity, databaseManager );
        RemoteMembersResolver hostnameResolver = ResolutionResolverFactory.chooseResolver( globalConfig, logService );
        RetryStrategy retryStrategy = resolveStrategy( globalConfig );
        return discoveryServiceFactory.readReplicaTopologyService( globalConfig, logProvider, jobScheduler, discoveryMember, hostnameResolver, retryStrategy,
                sslPolicyLoader );
    }

    private static RetryStrategy resolveStrategy( Config config )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        // we want to have more retries at the given frequency than there is time in a refresh period
        int numberOfRetries = pollingFrequencyWithinRefreshWindow + 1;
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        return new RetryStrategy( delayInMillis, (long) numberOfRetries );
    }

    ReadReplicaDatabaseFactory readReplicaDatabaseFactory()
    {
        return readReplicaDatabaseFactory;
    }
}
