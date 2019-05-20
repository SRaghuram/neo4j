/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.read_replica;

import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.CatchupPollingProcess;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpConnector.Encryption;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static java.util.stream.Collectors.joining;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.internal.helpers.AdvertisedSocketAddress.advertisedAddress;
import static org.neo4j.internal.helpers.ListenSocketAddress.listenAddress;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

public class ReadReplica implements ClusterMember
{
    public interface ReadReplicaGraphDatabaseFactory
    {
        ReadReplicaGraphDatabase create( File databaseDirectory, Config memberConfig,
                GraphDatabaseDependencies databaseDependencies, DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId );
    }

    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final File neo4jHome;
    private final DatabaseLayout defaultDatabaseLayout;
    private final int serverId;
    private final MemberId memberId;
    private final String boltAdvertisedSocketAddress;
    private final Config memberConfig;
    private ReadReplicaGraphDatabase readDatabase;
    private GraphDatabaseFacade defaultDatabase;
    private final Monitors monitors;
    private final ThreadGroup threadGroup;
    private final File databasesDirectory;
    private final ReadReplicaGraphDatabaseFactory dbFactory;
    private volatile boolean hasPanicked;

    public ReadReplica( File parentDir, int serverId, int boltPort, int httpPort, int txPort, int backupPort,
            int discoveryPort, DiscoveryServiceFactory discoveryServiceFactory,
            List<AdvertisedSocketAddress> coreMemberDiscoveryAddresses, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
            String advertisedAddress, String listenAddress, ReadReplicaGraphDatabaseFactory dbFactory )
    {
        this.serverId = serverId;
        this.memberId = new MemberId( UUID.randomUUID() );

        String initialHosts = coreMemberDiscoveryAddresses.stream().map( AdvertisedSocketAddress::toString )
                .collect( joining( "," ) );
        boltAdvertisedSocketAddress = advertisedAddress( advertisedAddress, boltPort );

        Map<String,String> config = stringMap();
        config.put( "dbms.mode", "READ_REPLICA" );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialHosts );
        config.put( CausalClusteringSettings.discovery_listen_address.name(), listenAddress( listenAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.discovery_advertised_address.name(), advertisedAddress( advertisedAddress, discoveryPort ) );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String,IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        config.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new BoltConnector( "bolt" ).listen_address.name(), listenAddress( listenAddress, boltPort ) );
        config.put( new BoltConnector( "bolt" ).advertised_address.name(), boltAdvertisedSocketAddress );
        config.put( new BoltConnector( "bolt" ).encryption_level.name(), DISABLED.name() );
        config.put( new HttpConnector( "http", Encryption.NONE ).type.name(), "HTTP" );
        config.put( new HttpConnector( "http", Encryption.NONE ).enabled.name(), "true" );
        config.put( new HttpConnector( "http", Encryption.NONE ).listen_address.name(), listenAddress( listenAddress, httpPort ) );
        config.put( new HttpConnector( "http", Encryption.NONE ).advertised_address.name(), advertisedAddress( advertisedAddress, httpPort ) );

        this.neo4jHome = new File( parentDir, "read-replica-" + serverId );
        config.put( GraphDatabaseSettings.neo4j_home.name(), neo4jHome.getAbsolutePath() );

        config.put( CausalClusteringSettings.transaction_listen_address.name(), listenAddress( listenAddress, txPort ) );
        config.put( OnlineBackupSettings.online_backup_listen_address.name(), listenAddress( listenAddress, backupPort ) );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );
        config.put( GraphDatabaseSettings.transaction_logs_root_path.name(), new File( neo4jHome, "replica-tx-logs-" + serverId ).getAbsolutePath() );
        memberConfig = Config.defaults( config );

        this.discoveryServiceFactory = discoveryServiceFactory;
        File dataDirectory = new File( neo4jHome, "data" );
        databasesDirectory = new File( dataDirectory, "databases" );
        this.monitors = monitors;
        threadGroup = new ThreadGroup( toString() );
        this.dbFactory = dbFactory;
        this.defaultDatabaseLayout = DatabaseLayout.of( databasesDirectory, of( memberConfig ), DEFAULT_DATABASE_NAME );
    }

    @Override
    public String boltAdvertisedAddress()
    {
        return boltAdvertisedSocketAddress;
    }

    @Override
    public MemberId id()
    {
        return memberId;
    }

    @Override
    public void start()
    {
        readDatabase =  dbFactory.create( databasesDirectory, memberConfig,
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory, memberId );
        defaultDatabase = (GraphDatabaseFacade) readDatabase.getManagementService().database( DEFAULT_DATABASE_NAME );
        PanicService panicService = defaultDatabase.getDependencyResolver().resolveDependency( PanicService.class );
        panicService.addPanicEventHandler( () -> hasPanicked = true );
    }

    @Override
    public void shutdown()
    {
        if ( readDatabase != null )
        {
            try
            {
                readDatabase.getManagementService().shutdown();
            }
            finally
            {
                readDatabase = null;
            }
        }
    }

    @Override
    public boolean isShutdown()
    {
        return readDatabase == null;
    }

    @Override
    public boolean hasPanicked()
    {
        return hasPanicked;
    }

    public CatchupPollingProcess txPollingClient()
    {
        return defaultDatabase.getDependencyResolver().resolveDependency( CatchupPollingProcess.class );
    }

    @Override
    public GraphDatabaseFacade defaultDatabase()
    {
        return defaultDatabase;
    }

    @Override
    public ClientConnectorAddresses clientConnectorAddresses()
    {
        return ClientConnectorAddresses.extractFromConfig( memberConfig );
    }

    @Override
    public String settingValue( String settingName )
    {
        return memberConfig.getRaw().get( settingName );
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

    @Override
    public DatabaseLayout databaseLayout()
    {
        return defaultDatabaseLayout;
    }

    @Override
    public String toString()
    {
        return "ReadReplica{serverId=" + serverId + ", memberId=" + memberId + "}";
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltAdvertisedSocketAddress );
    }

    @Override
    public File homeDir()
    {
        return neo4jHome;
    }

    public void setUpstreamDatabaseSelectionStrategy( String key )
    {
        updateConfig( CausalClusteringSettings.upstream_selection_strategy, key );
    }

    @Override
    public int serverId()
    {
        return serverId;
    }

    @Override
    public Config config()
    {
        return memberConfig;
    }
}
