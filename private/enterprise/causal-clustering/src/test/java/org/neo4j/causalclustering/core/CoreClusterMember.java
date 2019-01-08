/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.common.ClusterMember;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.error_handling.PanicService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Level;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.neo4j.helpers.AdvertisedSocketAddress.advertisedAddress;
import static org.neo4j.helpers.ListenSocketAddress.listenAddress;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CoreClusterMember implements ClusterMember<CoreGraphDatabase>
{
    private final File neo4jHome;
    protected final DiscoveryServiceFactory discoveryServiceFactory;
    private final File defaultDatabaseDirectory;
    private final File clusterStateDir;
    private final File raftLogDir;
    private final Map<String,String> config = stringMap();
    private final int serverId;
    private final String boltAdvertisedSocketAddress;
    private final int discoveryPort;
    private final String raftListenAddress;
    protected CoreGraphDatabase database;
    private final Config memberConfig;
    private final ThreadGroup threadGroup;
    private final Monitors monitors = new Monitors();
    private final String dbName;
    private final File databasesDirectory;
    private volatile boolean hasPanicked;

    public CoreClusterMember( int serverId,
                              int discoveryPort,
                              int txPort,
                              int raftPort,
                              int boltPort,
                              int httpPort,
                              int backupPort,
                              int clusterSize,
                              List<AdvertisedSocketAddress> addresses,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              String recordFormat,
                              File parentDir,
                              Map<String, String> extraParams,
                              Map<String, IntFunction<String>> instanceExtraParams,
                              String listenAddress,
                              String advertisedAddress )
    {
        this.serverId = serverId;

        this.discoveryPort = discoveryPort;

        String initialMembers = addresses.stream().map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );
        boltAdvertisedSocketAddress = advertisedAddress( advertisedAddress, boltPort );
        raftListenAddress = listenAddress( listenAddress, raftPort );

        config.put( GraphDatabaseSettings.active_database.name(), GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        config.put( CommercialEditionSettings.mode.name(), CommercialEditionSettings.Mode.CORE.name() );
        config.put( GraphDatabaseSettings.default_advertised_address.name(), advertisedAddress );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialMembers );
        config.put( CausalClusteringSettings.discovery_listen_address.name(), listenAddress( listenAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.discovery_advertised_address.name(), advertisedAddress( advertisedAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.transaction_listen_address.name(), listenAddress( listenAddress, txPort ) );
        config.put( CausalClusteringSettings.raft_listen_address.name(), raftListenAddress );
        config.put( CausalClusteringSettings.cluster_topology_refresh.name(), "1000ms" );
        config.put( CausalClusteringSettings.minimum_core_cluster_size_at_formation.name(), String.valueOf( clusterSize ) );
        config.put( CausalClusteringSettings.minimum_core_cluster_size_at_runtime.name(), String.valueOf( clusterSize ) );
        config.put( CausalClusteringSettings.leader_election_timeout.name(), "500ms" );
        config.put( CausalClusteringSettings.raft_messages_log_enable.name(), Settings.TRUE );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new BoltConnector( "bolt" ).listen_address.name(), listenAddress( listenAddress, boltPort ) );
        config.put( new BoltConnector( "bolt" ).advertised_address.name(), boltAdvertisedSocketAddress );
        config.put( new HttpConnector( "http", Encryption.NONE ).type.name(), "HTTP" );
        config.put( new HttpConnector( "http", Encryption.NONE ).enabled.name(), "true" );
        config.put( new HttpConnector( "http", Encryption.NONE ).listen_address.name(), listenAddress( listenAddress, httpPort ) );
        config.put( new HttpConnector( "http", Encryption.NONE ).advertised_address.name(), advertisedAddress( advertisedAddress, httpPort ) );
        config.put( OnlineBackupSettings.online_backup_listen_address.name(), listenAddress( listenAddress, backupPort ) );
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
        clusterStateDir = ClusterStateDirectory.withoutInitializing( null, dataDir ).get();
        raftLogDir = CoreStateFiles.RAFT_LOG.at( clusterStateDir );
        databasesDirectory = new File( dataDir, "databases" );
        defaultDatabaseDirectory = new File( databasesDirectory, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        memberConfig = Config.defaults( config );

        this.dbName = memberConfig.get( CausalClusteringSettings.database );

        //noinspection ResultOfMethodCallIgnored
        defaultDatabaseDirectory.mkdirs();
        threadGroup = new ThreadGroup( toString() );
    }

    public String boltAdvertisedAddress()
    {
        return boltAdvertisedSocketAddress;
    }

    public String routingURI()
    {
        return String.format( "bolt+routing://%s", boltAdvertisedSocketAddress );
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltAdvertisedSocketAddress );
    }

    public String raftListenAddress()
    {
        return raftListenAddress;
    }

    @Override
    public void start()
    {
        database = new CoreGraphDatabase( databasesDirectory, memberConfig,
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory );

        PanicService panicService = database.getDependencyResolver().resolveDependency( PanicService.class );
        panicService.addPanicEventHandler( () -> hasPanicked = true );
    }

    @Override
    public void shutdown()
    {
        if ( database != null )
        {
            try
            {
                database.shutdown();
            }
            finally
            {
                database = null;
            }
        }
    }

    @Override
    public boolean isShutdown()
    {
        return database == null;
    }

    @Override
    public boolean hasPanicked()
    {
        return hasPanicked;
    }

    @Override
    public CoreGraphDatabase database()
    {
        return database;
    }

    @Override
    public File databaseDirectory()
    {
        return defaultDatabaseDirectory;
    }

    protected File databasesDirectory()
    {
        return databasesDirectory;
    }

    public RaftLogPruner raftLogPruner()
    {
        return database.getDependencyResolver().resolveDependency( RaftLogPruner.class );
    }

    public RaftMachine raft()
    {
        return database.getDependencyResolver().resolveDependency( RaftMachine.class );
    }

    public MemberId id()
    {
        return database.getDependencyResolver().resolveDependency( RaftMachine.class ).identity();
    }

    public SortedMap<Long, File> getLogFileNames() throws IOException
    {
        File logFilesDir = CoreStateFiles.RAFT_LOG.at( clusterStateDir );
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            return new FileNames( logFilesDir ).getAllFiles( fileSystem, null );
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
        return format( "CoreClusterMember{serverId=%d}", serverId );
    }

    @Override
    public int serverId()
    {
        return serverId;
    }

    public String dbName()
    {
        return dbName;
    }

    @Override
    public ClientConnectorAddresses clientConnectorAddresses()
    {
        return ClientConnectorAddresses.extractFromConfig( Config.defaults( this.config ) );
    }

    @Override
    public String settingValue( String settingName )
    {
        return config.get(settingName);
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

    public File clusterStateDirectory()
    {
        return clusterStateDir;
    }

    public File raftLogDirectory()
    {
        return raftLogDir;
    }

    public void disableCatchupServer() throws Throwable
    {
        database.disableCatchupServer();
    }

    public int discoveryPort()
    {
        return discoveryPort;
    }
}
