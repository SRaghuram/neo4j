/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import akka.cluster.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.OnlineBackupSettings;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.causalclustering.common.Cluster.TOPOLOGY_REFRESH_INTERVAL;
import static com.neo4j.configuration.CausalClusteringSettings.SelectionStrategies.NO_BALANCING;
import static java.lang.Boolean.TRUE;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;

public class CoreClusterMember implements ClusterMember
{
    private GlobalModule globalModule;

    public interface CoreGraphDatabaseFactory
    {
        CoreGraphDatabase create( Config memberConfig, GraphDatabaseDependencies databaseDependencies,
                DiscoveryServiceFactory discoveryServiceFactory );
    }

    private final Path neo4jHome;
    private final Neo4jLayout neo4jLayout;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final DatabaseLayout defaultDatabaseLayout;
    private final ClusterStateLayout clusterStateLayout;
    private final Config.Builder config = Config.newBuilder();
    private final int index;
    private final String boltSocketAddress;
    private final String intraClusterBoltSocketAddress;
    private final int discoveryPort;
    private final String raftListenAddress;
    private CoreGraphDatabase coreGraphDatabase;
    private GraphDatabaseFacade defaultDatabase;
    private GraphDatabaseFacade systemDatabase;
    private final Config memberConfig;
    private final ThreadGroup threadGroup;
    private final Monitors monitors = new Monitors();
    private final CoreGraphDatabaseFactory dbFactory =
            ( Config config, GraphDatabaseDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory ) ->
                    new TestCoreGraphDatabase( config, dependencies, discoveryServiceFactory, this::coreEditionModuleSupplier );
    private DatabaseIdRepository databaseIdRepository;

    public CoreClusterMember( int index,
                              int discoveryPort,
                              int txPort,
                              int raftPort,
                              int boltPort,
                              int intraClusterBoltPort,
                              int httpPort,
                              int backupPort,
                              int clusterSize,
                              List<SocketAddress> addresses,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              String recordFormat,
                              Path parentDir,
                              Map<String, String> extraParams,
                              Map<String, IntFunction<String>> instanceExtraParams,
                              String listenAddress,
                              String advertisedAddress )
    {
        this.index = index;

        this.discoveryPort = discoveryPort;

        boltSocketAddress = format( advertisedAddress, boltPort );
        intraClusterBoltSocketAddress = format( advertisedAddress, intraClusterBoltPort );
        raftListenAddress = format( listenAddress, raftPort );

        config.set( default_database, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        config.set( GraphDatabaseSettings.mode, GraphDatabaseSettings.Mode.CORE );
        config.set( GraphDatabaseSettings.default_advertised_address, new SocketAddress( advertisedAddress ) );
        config.set( CausalClusteringSettings.initial_discovery_members, addresses );
        config.set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( listenAddress, discoveryPort ) );
        config.set( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( advertisedAddress, discoveryPort ) );
        config.set( CausalClusteringSettings.transaction_listen_address, new SocketAddress( listenAddress, txPort ) );
        config.set( CausalClusteringSettings.transaction_advertised_address, new SocketAddress( txPort ) );
        config.set( CausalClusteringSettings.raft_listen_address, new SocketAddress( listenAddress, raftPort ) );
        config.set( CausalClusteringSettings.raft_advertised_address, new SocketAddress( raftPort ) );
        config.set( CausalClusteringSettings.cluster_topology_refresh, TOPOLOGY_REFRESH_INTERVAL );
        config.set( CausalClusteringSettings.minimum_core_cluster_size_at_formation, clusterSize );
        config.set( CausalClusteringSettings.minimum_core_cluster_size_at_runtime, clusterSize );
        config.set( CausalClusteringSettings.leader_balancing, NO_BALANCING );
        config.set( CausalClusteringInternalSettings.raft_messages_log_enable, TRUE );
        config.set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG );
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( BoltConnector.enabled, TRUE );
        config.set( BoltConnector.listen_address, new SocketAddress( listenAddress, boltPort ) );
        config.set( BoltConnector.advertised_address, new SocketAddress( advertisedAddress, boltPort ) );
        config.set( GraphDatabaseSettings.routing_listen_address, new SocketAddress( listenAddress, intraClusterBoltPort ) );
        config.set( GraphDatabaseSettings.routing_advertised_address, new SocketAddress( advertisedAddress, intraClusterBoltPort ) );
        config.set( BoltConnector.encryption_level, DISABLED );
        config.set( HttpConnector.enabled, TRUE );
        config.set( HttpConnector.listen_address, new SocketAddress( listenAddress, httpPort ) );
        config.set( HttpConnector.advertised_address, new SocketAddress( advertisedAddress, httpPort ) );
        config.set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( listenAddress, backupPort ) );
        config.set( GraphDatabaseSettings.pagecache_memory, "8m" );
        config.set( GraphDatabaseInternalSettings.auth_store, parentDir.resolve( "auth" ).toAbsolutePath() );
        config.set( GraphDatabaseInternalSettings.transaction_start_timeout, Duration.ZERO );
        config.set( CausalClusteringInternalSettings.experimental_raft_protocol, true );
        config.set( CausalClusteringInternalSettings.experimental_catchup_protocol, true );
        config.set( CausalClusteringInternalSettings.middleware_akka_seed_node_timeout, Duration.ofSeconds( 6 ));
        config.setRaw( extraParams );

        Map<String,String> instanceExtras = new HashMap<>();
        instanceExtraParams.forEach( ( setting, function ) -> instanceExtras.put( setting, function.apply( index ) ) );
        config.setRaw( instanceExtras );

        this.neo4jHome = parentDir.resolve( "server-core-" + index );
        config.set( GraphDatabaseSettings.neo4j_home, neo4jHome.toAbsolutePath() );
        config.set( GraphDatabaseSettings.transaction_logs_root_path, neo4jHome.resolve( "core-tx-logs-" + index ).toAbsolutePath() );
        memberConfig = config.build();

        this.discoveryServiceFactory = discoveryServiceFactory;
        clusterStateLayout = ClusterStateLayout.of( memberConfig.get( CausalClusteringSettings.cluster_state_directory ) );

        threadGroup = new ThreadGroup( toString() );
        this.neo4jLayout = Neo4jLayout.of( memberConfig );
        this.defaultDatabaseLayout = neo4jLayout.databaseLayout( memberConfig.get( default_database ) );
    }

    private CoreEditionModule coreEditionModuleSupplier( final GlobalModule globalModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        this.globalModule = globalModule;
        return new CoreEditionModule( globalModule, discoveryServiceFactory );
    }

    @Override
    public String boltAdvertisedAddress()
    {
        return boltSocketAddress;
    }

    @Override
    public String intraClusterBoltAdvertisedAddress()
    {
        return intraClusterBoltSocketAddress;
    }

    public String routingURI()
    {
        return String.format( "neo4j://%s", boltSocketAddress );
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltSocketAddress );
    }

    public String raftListenAddress()
    {
        return raftListenAddress;
    }

    @Override
    public ServerId serverId()
    {
        return systemDatabase.getDependencyResolver().resolveDependency( ClusteringIdentityModule.class ).myself();
    }

    public RaftMemberId raftMemberIdFor( NamedDatabaseId databaseId )
    {
        return systemDatabase.getDependencyResolver().resolveDependency( ClusteringIdentityModule.class ).memberId( databaseId );
    }

    @Override
    public void start()
    {
        coreGraphDatabase = dbFactory.create( memberConfig,
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory );
        defaultDatabase = (GraphDatabaseFacade) coreGraphDatabase.getManagementService().database( config().get( default_database ) );
        systemDatabase = (GraphDatabaseFacade) coreGraphDatabase.getManagementService().database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
    }

    @Override
    public void shutdown()
    {
        if ( coreGraphDatabase != null )
        {
            try
            {
                coreGraphDatabase.getManagementService().shutdown();
            }
            finally
            {
                coreGraphDatabase = null;
                defaultDatabase = null;
                databaseIdRepository = null;
            }
        }
    }

    @Override
    public boolean isShutdown()
    {
        return coreGraphDatabase == null;
    }

    @Override
    public DatabaseManagementService managementService()
    {
        if ( coreGraphDatabase == null )
        {
            return null;
        }
        return coreGraphDatabase.getManagementService();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return defaultDatabaseLayout;
    }

    public SortedMap<Long, Path> getRaftLogFileNames( String databaseName ) throws IOException
    {
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            return new FileNames( raftLogDirectory( databaseName ) ).getAllFiles( fileSystem, null );
        }
    }

    @Override
    public Neo4jLayout neo4jLayout()
    {
        return neo4jLayout;
    }

    @Override
    public Path homePath()
    {
        return neo4jHome;
    }

    @Override
    public final String toString()
    {
        return "CoreClusterMember{index=" + index + ", serverId=" + (systemDatabase == null ? null : serverId()) + "}";
    }

    @Override
    public int index()
    {
        return index;
    }

    public NamedDatabaseId databaseId()
    {
        return defaultDatabase.getDependencyResolver().resolveDependency( Database.class ).getNamedDatabaseId();
    }

    @Override
    public ConnectorAddresses clientConnectorAddresses()
    {
        return ConnectorAddresses.fromConfig( config.build() );
    }

    @Override
    public <T> T settingValue( Setting<T> setting )
    {
        return memberConfig.get( setting );
    }

    @Override
    public Config config()
    {
        return memberConfig;
    }

    @Override
    public ThreadGroup threadGroup()
    {
        return threadGroup;
    }

    @Override
    public Monitors monitors()
    {
        return monitors;
    }

    public ClusterStateLayout clusterStateLayout()
    {
        return clusterStateLayout;
    }

    public Path clusterStateDirectory()
    {
        return clusterStateLayout.getClusterStateDirectory();
    }

    public Path raftLogDirectory( String databaseName )
    {
        return clusterStateLayout.raftLogDirectory( databaseName );
    }

    public int discoveryPort()
    {
        return discoveryPort;
    }

    public NamedDatabaseId databaseId( String databaseName )
    {
        if ( defaultDatabase == null )
        {
            throw new IllegalStateException( "defaultDatabase must not be null" );
        }
        else if ( databaseIdRepository == null )
        {
            DatabaseManager<?> databaseManager = defaultDatabase.getDependencyResolver().resolveDependency( DatabaseManager.class );
            databaseIdRepository = databaseManager.databaseIdRepository();
        }
        return databaseIdRepository.getByName( databaseName ).orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: " + databaseName ) );
    }

    public void unbind( FileSystemAbstraction fs ) throws IOException
    {
        fs.deleteRecursively( clusterStateLayout.getClusterStateDirectory() );
        fs.deleteFile( neo4jLayout.serverIdFile() );
    }

    public Optional<Cluster> getAkkaCluster()
    {
        if ( globalModule == null )
        {
            return Optional.empty();
        }
        try
        {
            var coreTopologyService = globalModule.getGlobalDependencies().resolveDependency( CoreTopologyService.class );
            Cluster akkaCluster = ((AkkaCoreTopologyService) coreTopologyService).getAkkaCluster();
            return Optional.ofNullable( akkaCluster );
        }
        catch ( UnsatisfiedDependencyException | NullPointerException ignored )
        {
            return Optional.empty();
        }
    }
}
