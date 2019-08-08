/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;

public class CoreClusterMember implements ClusterMember
{
    public interface CoreGraphDatabaseFactory
    {
        CoreGraphDatabase create( File databaseDirectory, Config memberConfig, GraphDatabaseDependencies databaseDependencies,
                DiscoveryServiceFactory discoveryServiceFactory );
    }

    private final File neo4jHome;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final DatabaseLayout defaultDatabaseLayout;
    private final ClusterStateLayout clusterStateLayout;
    private final Config.Builder config = Config.newBuilder();
    private final int serverId;
    private final String boltSocketAddress;
    private final int discoveryPort;
    private final String raftListenAddress;
    private CoreGraphDatabase coreGraphDatabase;
    private GraphDatabaseFacade defaultDatabase;
    private GraphDatabaseFacade systemDatabase;
    private final Config memberConfig;
    private final ThreadGroup threadGroup;
    private final Monitors monitors = new Monitors();
    private final File databasesDirectory;
    private final CoreGraphDatabaseFactory dbFactory;
    private volatile boolean hasPanicked;
    private DatabaseIdRepository databaseIdRepository;

    public CoreClusterMember( int serverId,
                              int discoveryPort,
                              int txPort,
                              int raftPort,
                              int boltPort,
                              int httpPort,
                              int backupPort,
                              int clusterSize,
                              List<SocketAddress> addresses,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              String recordFormat,
                              File parentDir,
                              Map<String, String> extraParams,
                              Map<String, IntFunction<String>> instanceExtraParams,
                              String listenAddress,
                              String advertisedAddress,
                              CoreGraphDatabaseFactory dbFactory )
    {
        this.serverId = serverId;

        this.discoveryPort = discoveryPort;

        boltSocketAddress = format( advertisedAddress, boltPort );
        raftListenAddress = format( listenAddress, raftPort );

        config.set( GraphDatabaseSettings.default_database, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        config.set( CommercialEditionSettings.mode, CommercialEditionSettings.Mode.CORE );
        config.set( GraphDatabaseSettings.default_advertised_address, new SocketAddress( advertisedAddress ) );
        config.set( CausalClusteringSettings.initial_discovery_members, addresses );
        config.set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( listenAddress, discoveryPort ) );
        config.set( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( advertisedAddress, discoveryPort ) );
        config.set( CausalClusteringSettings.transaction_listen_address, new SocketAddress( listenAddress, txPort ) );
        config.set( CausalClusteringSettings.transaction_advertised_address, new SocketAddress( txPort ) );
        config.set( CausalClusteringSettings.raft_listen_address, new SocketAddress( listenAddress, raftPort ) );
        config.set( CausalClusteringSettings.raft_advertised_address, new SocketAddress( raftPort ) );
        config.set( CausalClusteringSettings.cluster_topology_refresh, Duration.ofMillis( 1000 ) );
        config.set( CausalClusteringSettings.minimum_core_cluster_size_at_formation, clusterSize );
        config.set( CausalClusteringSettings.minimum_core_cluster_size_at_runtime, clusterSize );
        config.set( CausalClusteringSettings.leader_election_timeout, Duration.ofMillis( 500 ) );
        config.set( CausalClusteringSettings.raft_messages_log_enable, true );
        config.set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG );
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( BoltConnector.enabled, true );
        config.set( BoltConnector.listen_address, new SocketAddress( listenAddress, boltPort ) );
        config.set( BoltConnector.advertised_address, new SocketAddress( advertisedAddress, boltPort ) );
        config.set( BoltConnector.encryption_level, DISABLED );
        config.set( HttpConnector.enabled, true );
        config.set( HttpConnector.listen_address, new SocketAddress( listenAddress, httpPort ) );
        config.set( HttpConnector.advertised_address, new SocketAddress( advertisedAddress, httpPort ) );
        config.set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( listenAddress, backupPort ) );
        config.set( GraphDatabaseSettings.pagecache_memory, "8m" );
        config.set( GraphDatabaseSettings.auth_store, new File( parentDir, "auth" ).toPath().toAbsolutePath() );
        config.setRaw( extraParams );

        Map<String,String> instanceExtras = new HashMap<>();
        instanceExtraParams.forEach( ( setting, function ) -> instanceExtras.put( setting, function.apply( serverId ) ) );
        config.setRaw( instanceExtras );

        this.neo4jHome = new File( parentDir, "server-core-" + serverId );
        config.set( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath().toAbsolutePath() );
        config.set( GraphDatabaseSettings.logs_directory, new File( neo4jHome, "logs" ).toPath().toAbsolutePath() );
        config.set( GraphDatabaseSettings.transaction_logs_root_path, new File( parentDir, "core-tx-logs-" + serverId ).toPath().toAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        File dataDir = new File( neo4jHome, "data" );
        clusterStateLayout = ClusterStateLayout.of( dataDir );
        databasesDirectory = new File( dataDir, "databases" );
        memberConfig = config.build();

        threadGroup = new ThreadGroup( toString() );
        this.dbFactory = dbFactory;
        this.defaultDatabaseLayout = DatabaseLayout.of( databasesDirectory, of( memberConfig ), GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    @Override
    public String boltAdvertisedAddress()
    {
        return boltSocketAddress;
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
    public MemberId id()
    {
        return defaultDatabase.getDependencyResolver().resolveDependency( RaftMachine.class ).identity();
    }

    @Override
    public void start()
    {
        coreGraphDatabase = dbFactory.create( databasesDirectory, memberConfig,
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory );
        defaultDatabase = (GraphDatabaseFacade) coreGraphDatabase.getManagementService().database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        systemDatabase = (GraphDatabaseFacade) coreGraphDatabase.getManagementService().database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );

        DependencyResolver deps = systemDatabase.getDependencyResolver();
        PanicService panicService = deps.resolveDependency( PanicService.class );
        panicService.addPanicEventHandler( () -> hasPanicked = true );
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
    public boolean hasPanicked()
    {
        return hasPanicked;
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
    public GraphDatabaseFacade defaultDatabase()
    {
        return defaultDatabase;
    }

    @Override
    public GraphDatabaseFacade systemDatabase()
    {
        return systemDatabase;
    }

    @Override
    public GraphDatabaseFacade database( String databaseName )
    {
        return (GraphDatabaseFacade) coreGraphDatabase.getManagementService().database( databaseName );
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return defaultDatabaseLayout;
    }

    public <T> T resolveDependency( String databaseName, Class<T> type )
    {
        return ((GraphDatabaseFacade) coreGraphDatabase.getManagementService().database( databaseName )).getDependencyResolver().resolveDependency( type );
    }

    public SortedMap<Long, File> getLogFileNames() throws IOException
    {
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            return new FileNames( raftLogDirectory() ).getAllFiles( fileSystem, null );
        }
    }

    @Override
    public File homeDir()
    {
        return neo4jHome;
    }

    @Override
    public String toString()
    {
        return "CoreClusterMember{serverId=" + serverId + ", memberId=" + (coreGraphDatabase == null ? null : id()) + "}";
    }

    @Override
    public int serverId()
    {
        return serverId;
    }

    public DatabaseId databaseId()
    {
        return defaultDatabase.getDependencyResolver().resolveDependency( Database.class ).getDatabaseId();
    }

    @Override
    public ClientConnectorAddresses clientConnectorAddresses()
    {
        return ClientConnectorAddresses.extractFromConfig( config.build() );
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

    public File clusterStateDirectory()
    {
        return clusterStateLayout.getClusterStateDirectory();
    }

    public File raftLogDirectory()
    {
        var defaultDatabase = config().get( GraphDatabaseSettings.default_database );
        return clusterStateLayout.raftLogDirectory( defaultDatabase );
    }

    public int discoveryPort()
    {
        return discoveryPort;
    }

    public DatabaseId databaseId( String databaseName )
    {
        if ( defaultDatabase == null )
        {
            throw new IllegalStateException( "defaultDatabase must not be null" );
        }
        else if ( databaseIdRepository == null )
        {
            DatabaseManager databaseManager = defaultDatabase.getDependencyResolver().resolveDependency( DatabaseManager.class );
            databaseIdRepository = databaseManager.databaseIdRepository();
        }
        return databaseIdRepository.get( databaseName );
    }
}
