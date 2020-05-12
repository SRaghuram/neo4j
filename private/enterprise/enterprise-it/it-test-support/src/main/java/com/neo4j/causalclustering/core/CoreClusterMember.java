/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.causalclustering.common.Cluster.TOPOLOGY_REFRESH_INTERVAL;
import static java.lang.Boolean.TRUE;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;

public class CoreClusterMember implements ClusterMember
{
    public interface CoreGraphDatabaseFactory
    {
        CoreGraphDatabase create( Config memberConfig, GraphDatabaseDependencies databaseDependencies,
                DiscoveryServiceFactory discoveryServiceFactory );
    }

    private final File neo4jHome;
    private final Neo4jLayout neo4jLayout;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final DatabaseLayout defaultDatabaseLayout;
    private final ClusterStateLayout clusterStateLayout;
    private final Config.Builder config = Config.newBuilder();
    private final int serverId;
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
    private final CoreGraphDatabaseFactory dbFactory;
    private DatabaseIdRepository databaseIdRepository;

    public CoreClusterMember( int serverId,
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
        intraClusterBoltSocketAddress = format( advertisedAddress, intraClusterBoltPort );
        raftListenAddress = format( listenAddress, raftPort );

        config.set( default_database, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        config.set( EnterpriseEditionSettings.mode, EnterpriseEditionSettings.Mode.CORE );
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
        config.set( CausalClusteringSettings.raft_messages_log_enable, TRUE );
        config.set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG );
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( BoltConnector.enabled, TRUE );
        config.set( BoltConnector.listen_address, new SocketAddress( listenAddress, boltPort ) );
        config.set( BoltConnector.advertised_address, new SocketAddress( advertisedAddress, boltPort ) );
        config.set( BoltConnector.connector_routing_listen_address, new SocketAddress( listenAddress, intraClusterBoltPort ) );
        config.set( BoltConnector.connector_routing_advertised_address, new SocketAddress( advertisedAddress, intraClusterBoltPort ) );
        config.set( BoltConnector.encryption_level, DISABLED );
        config.set( HttpConnector.enabled, TRUE );
        config.set( HttpConnector.listen_address, new SocketAddress( listenAddress, httpPort ) );
        config.set( HttpConnector.advertised_address, new SocketAddress( advertisedAddress, httpPort ) );
        config.set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( listenAddress, backupPort ) );
        config.set( GraphDatabaseSettings.pagecache_memory, "8m" );
        config.set( GraphDatabaseSettings.auth_store, parentDir.toPath().resolve( "auth" ).toAbsolutePath() );
        config.set( GraphDatabaseSettings.transaction_start_timeout, Duration.ZERO );
        config.setRaw( extraParams );

        Map<String,String> instanceExtras = new HashMap<>();
        instanceExtraParams.forEach( ( setting, function ) -> instanceExtras.put( setting, function.apply( serverId ) ) );
        config.setRaw( instanceExtras );

        this.neo4jHome = new File( parentDir, "server-core-" + serverId );
        config.set( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath().toAbsolutePath() );
        config.set( GraphDatabaseSettings.transaction_logs_root_path, neo4jHome.toPath().resolve( "core-tx-logs-" + serverId ).toAbsolutePath() );
        memberConfig = config.build();

        this.discoveryServiceFactory = discoveryServiceFactory;
        clusterStateLayout = ClusterStateLayout.of( memberConfig.get( data_directory ).toFile() );

        threadGroup = new ThreadGroup( toString() );
        this.dbFactory = dbFactory;
        this.neo4jLayout = Neo4jLayout.of( memberConfig );
        this.defaultDatabaseLayout = neo4jLayout.databaseLayout( memberConfig.get( default_database ) );
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
    public MemberId id()
    {
        return systemDatabase.getDependencyResolver().resolveDependency( RaftMachine.class ).memberId();
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

    public SortedMap<Long, File> getRaftLogFileNames( String databaseName ) throws IOException
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
    public File homeDir()
    {
        return neo4jHome;
    }

    @Override
    public final String toString()
    {
        return "CoreClusterMember{serverId=" + serverId + ", memberId=" + (coreGraphDatabase == null ? null : id()) + "}";
    }

    @Override
    public int serverId()
    {
        return serverId;
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

    public File clusterStateDirectory()
    {
        return clusterStateLayout.getClusterStateDirectory();
    }

    public File raftLogDirectory( String databaseName )
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
            DatabaseManager databaseManager = defaultDatabase.getDependencyResolver().resolveDependency( DatabaseManager.class );
            databaseIdRepository = databaseManager.databaseIdRepository();
        }
        return databaseIdRepository.getByName( databaseName ).orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: " + databaseName ) );
    }
}
