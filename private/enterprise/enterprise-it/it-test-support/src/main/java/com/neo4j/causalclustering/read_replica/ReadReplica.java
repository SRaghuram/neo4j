/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.read_replica;

import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.OnlineBackupSettings;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.causalclustering.common.Cluster.TOPOLOGY_REFRESH_INTERVAL;
import static java.lang.Boolean.TRUE;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

public class ReadReplica implements ClusterMember
{
    public interface ReadReplicaGraphDatabaseFactory
    {
        ReadReplicaGraphDatabase create( Config memberConfig, GraphDatabaseDependencies databaseDependencies,
                DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId );
    }

    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final File neo4jHome;
    private final Neo4jLayout neo4jLayout;
    private final DatabaseLayout defaultDatabaseLayout;
    private final int serverId;
    private final MemberId memberId;
    private final String boltSocketAddress;
    private final String intraClusterBoltSocketAddress;
    private final Config memberConfig;
    private final Monitors monitors;
    private final ThreadGroup threadGroup;
    private final ReadReplicaGraphDatabaseFactory dbFactory;

    private ReadReplicaGraphDatabase readReplicaGraphDatabase;

    public ReadReplica( File parentDir, int serverId, int boltPort, int intraClusterBoltPort, int httpPort,
            int txPort, int backupPort, int discoveryPort, DiscoveryServiceFactory discoveryServiceFactory,
            List<SocketAddress> coreMemberDiscoveryAddresses, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
            String advertisedAddress, String listenAddress, ReadReplicaGraphDatabaseFactory dbFactory )
    {
        this.serverId = serverId;
        this.memberId = new MemberId( UUID.randomUUID() );

        boltSocketAddress = format( advertisedAddress, boltPort );
        intraClusterBoltSocketAddress = format( advertisedAddress, intraClusterBoltPort);

        Config.Builder config = Config.newBuilder();
        config.set( GraphDatabaseSettings.mode, GraphDatabaseSettings.Mode.READ_REPLICA );
        config.set( CausalClusteringSettings.initial_discovery_members, coreMemberDiscoveryAddresses );
        config.set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( listenAddress, discoveryPort ) );
        config.set( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( advertisedAddress, discoveryPort ) );
        config.set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG );
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( GraphDatabaseSettings.pagecache_memory, "8m" );
        config.set( GraphDatabaseInternalSettings.auth_store, parentDir.toPath().resolve( "auth" ).toAbsolutePath() );
        config.set( GraphDatabaseInternalSettings.transaction_start_timeout, Duration.ZERO );
        config.setRaw( extraParams );

        Map<String,String> instanceExtras = new HashMap<>();
        instanceExtraParams.forEach( ( setting, function ) -> instanceExtras.put( setting, function.apply( serverId ) ) );
        config.setRaw( instanceExtras );

        config.set( BoltConnector.enabled, TRUE );
        config.set( BoltConnector.listen_address, new SocketAddress( listenAddress, boltPort ) );
        config.set( BoltConnector.advertised_address, new SocketAddress( advertisedAddress, boltPort ) );
        config.set( BoltConnector.connector_routing_listen_address, new SocketAddress( listenAddress, intraClusterBoltPort ) );
        config.set( BoltConnector.connector_routing_advertised_address, new SocketAddress( advertisedAddress, intraClusterBoltPort ) );
        config.set( BoltConnector.encryption_level, DISABLED );
        config.set( HttpConnector.enabled, TRUE );
        config.set( HttpConnector.listen_address, new SocketAddress( listenAddress, httpPort ) );
        config.set( HttpConnector.advertised_address, new SocketAddress( advertisedAddress, httpPort ) );

        this.neo4jHome = new File( parentDir, "read-replica-" + serverId );
        config.set( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath().toAbsolutePath() );

        config.set( CausalClusteringSettings.transaction_listen_address, new SocketAddress( listenAddress, txPort ) );
        config.set( CausalClusteringSettings.transaction_advertised_address, new SocketAddress( txPort ) );
        config.set( CausalClusteringSettings.cluster_topology_refresh, TOPOLOGY_REFRESH_INTERVAL );
        config.set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( listenAddress, backupPort ) );
        config.set( GraphDatabaseSettings.transaction_logs_root_path, neo4jHome.toPath().resolve( "replica-tx-logs-" + serverId ).toAbsolutePath() );
        memberConfig = config.build();

        this.discoveryServiceFactory = discoveryServiceFactory;
        this.monitors = monitors;
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

    @Override
    public MemberId id()
    {
        return memberId;
    }

    @Override
    public void start()
    {
        readReplicaGraphDatabase = dbFactory.create( memberConfig, newDependencies().monitors( monitors ), discoveryServiceFactory,
                memberId );
    }

    @Override
    public void shutdown()
    {
        if ( readReplicaGraphDatabase == null )
        {
            // already shutdown
            return;
        }

        try
        {
            readReplicaGraphDatabase.getManagementService().shutdown();
        }
        finally
        {
            readReplicaGraphDatabase = null;
        }
    }

    @Override
    public boolean isShutdown()
    {
        return readReplicaGraphDatabase == null;
    }

    @Override
    public DatabaseManagementService managementService()
    {
        return readReplicaGraphDatabase.getManagementService();
    }

    @Override
    public ConnectorAddresses clientConnectorAddresses()
    {
        return ConnectorAddresses.fromConfig( memberConfig );
    }

    @Override
    public <T> T settingValue( Setting<T> setting )
    {
        return memberConfig.get( setting );
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
    public final String toString()
    {
        return "ReadReplica{serverId=" + serverId + ", memberId=" + memberId + "}";
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltSocketAddress );
    }

    @Override
    public File homeDir()
    {
        return neo4jHome;
    }

    @Override
    public Neo4jLayout neo4jLayout()
    {
        return neo4jLayout;
    }

    public void setUpstreamDatabaseSelectionStrategy( String... strategies )
    {
        updateConfig( CausalClusteringSettings.upstream_selection_strategy, List.of( strategies ) );
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
