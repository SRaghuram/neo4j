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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static java.util.stream.Collectors.joining;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

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
    private final Map<String,String> config = stringMap();
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
    private final DatabaseIdRepository databaseIdRepository;
    private volatile boolean hasPanicked;

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

        String initialMembers = addresses.stream().map( SocketAddress::toString ).collect( joining( "," ) );
        boltSocketAddress = format( advertisedAddress, boltPort );
        raftListenAddress = format( listenAddress, raftPort );

        config.put( GraphDatabaseSettings.default_database.name(), GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        config.put( CommercialEditionSettings.mode.name(), CommercialEditionSettings.Mode.CORE.name() );
        config.put( GraphDatabaseSettings.default_advertised_address.name(), advertisedAddress );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialMembers );
        config.put( CausalClusteringSettings.discovery_listen_address.name(), format( listenAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.discovery_advertised_address.name(), format( advertisedAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.transaction_listen_address.name(), format( listenAddress, txPort ) );
        config.put( CausalClusteringSettings.raft_listen_address.name(), raftListenAddress );
        config.put( CausalClusteringSettings.cluster_topology_refresh.name(), "1000ms" );
        config.put( CausalClusteringSettings.minimum_core_cluster_size_at_formation.name(), String.valueOf( clusterSize ) );
        config.put( CausalClusteringSettings.minimum_core_cluster_size_at_runtime.name(), String.valueOf( clusterSize ) );
        config.put( CausalClusteringSettings.leader_election_timeout.name(), "500ms" );
        config.put( CausalClusteringSettings.raft_messages_log_enable.name(), TRUE );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( BoltConnector.enabled.name(), TRUE );
        config.put( BoltConnector.listen_address.name(), format( listenAddress, boltPort ) );
        config.put( BoltConnector.advertised_address.name(), boltSocketAddress );
        config.put( BoltConnector.encryption_level.name(), DISABLED.name() );
        config.put( HttpConnector.enabled.name(), TRUE );
        config.put( HttpConnector.listen_address.name(), format( listenAddress, httpPort ) );
        config.put( HttpConnector.advertised_address.name(), format( advertisedAddress, httpPort ) );
        config.put( OnlineBackupSettings.online_backup_listen_address.name(), format( listenAddress, backupPort ) );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        this.neo4jHome = new File( parentDir, "server-core-" + serverId );
        config.put( GraphDatabaseSettings.neo4j_home.name(), neo4jHome.getAbsolutePath() );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );
        config.put( GraphDatabaseSettings.transaction_logs_root_path.name(), new File( parentDir, "core-tx-logs-" + serverId ).getAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        File dataDir = new File( neo4jHome, "data" );
        clusterStateLayout = ClusterStateLayout.of( dataDir );
        databasesDirectory = new File( dataDir, "databases" );
        memberConfig = Config.defaults( config );
        this.databaseIdRepository = new PlaceholderDatabaseIdRepository( memberConfig );

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
        return ClientConnectorAddresses.extractFromConfig( Config.defaults( this.config ) );
    }

    @Override
    public <T> T settingValue( Setting<T> setting )
    {
        return ((SettingImpl<T>) setting).parse( config.get( setting.name() ) );
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
        DatabaseId defaultDatabaseId = databaseIdRepository.defaultDatabase();
        return clusterStateLayout.raftLogDirectory( defaultDatabaseId );
    }

    public int discoveryPort()
    {
        return discoveryPort;
    }

    public DatabaseIdRepository databaseIdRepository()
    {
        return databaseIdRepository;
    }
}
