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
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

public class ReadReplica implements ClusterMember
{
    public interface ReadReplicaGraphDatabaseFactory
    {
        ReadReplicaGraphDatabase create( File databaseDirectory, Config memberConfig, GraphDatabaseDependencies databaseDependencies,
                DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId );
    }

    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final File neo4jHome;
    private final DatabaseLayout defaultDatabaseLayout;
    private final int serverId;
    private final MemberId memberId;
    private final String boltSocketAddress;
    private final Config memberConfig;
    private final Monitors monitors;
    private final ThreadGroup threadGroup;
    private final File databasesDirectory;
    private final ReadReplicaGraphDatabaseFactory dbFactory;

    private ReadReplicaGraphDatabase readReplicaGraphDatabase;

    public ReadReplica( File parentDir, int serverId, int boltPort, int httpPort, int txPort, int backupPort,
            int discoveryPort, DiscoveryServiceFactory discoveryServiceFactory,
            List<SocketAddress> coreMemberDiscoveryAddresses, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
            String advertisedAddress, String listenAddress, ReadReplicaGraphDatabaseFactory dbFactory )
    {
        this.serverId = serverId;
        this.memberId = new MemberId( UUID.randomUUID() );

        boltSocketAddress = format( advertisedAddress, boltPort );

        Config.Builder config = Config.newBuilder();
        config.set( EnterpriseEditionSettings.mode, EnterpriseEditionSettings.Mode.READ_REPLICA );
        config.set( CausalClusteringSettings.initial_discovery_members, coreMemberDiscoveryAddresses );
        config.set( CausalClusteringSettings.discovery_listen_address, new SocketAddress( listenAddress, discoveryPort ) );
        config.set( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( advertisedAddress, discoveryPort ) );
        config.set( GraphDatabaseSettings.store_internal_log_level, Level.DEBUG );
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( GraphDatabaseSettings.pagecache_memory, "8m" );
        config.set( GraphDatabaseSettings.auth_store, new File( parentDir, "auth" ).toPath().toAbsolutePath() );
        config.setRaw( extraParams );

        Map<String,String> instanceExtras = new HashMap<>();
        instanceExtraParams.forEach( ( setting, function ) -> instanceExtras.put( setting, function.apply( serverId ) ) );
        config.setRaw( instanceExtras );

        config.set( BoltConnector.enabled, true );
        config.set( BoltConnector.listen_address, new SocketAddress( listenAddress, boltPort ) );
        config.set( BoltConnector.advertised_address, new SocketAddress( advertisedAddress, boltPort ) );
        config.set( BoltConnector.encryption_level, DISABLED );
        config.set( HttpConnector.enabled, true );
        config.set( HttpConnector.listen_address, new SocketAddress( listenAddress, httpPort ) );
        config.set( HttpConnector.advertised_address, new SocketAddress( advertisedAddress, httpPort ) );

        this.neo4jHome = new File( parentDir, "read-replica-" + serverId );
        config.set( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath().toAbsolutePath() );

        config.set( CausalClusteringSettings.transaction_listen_address, new SocketAddress( listenAddress, txPort ) );
        config.set( CausalClusteringSettings.transaction_advertised_address, new SocketAddress( txPort ) );
        config.set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( listenAddress, backupPort ) );
        config.set( GraphDatabaseSettings.logs_directory, new File( neo4jHome, "logs" ).toPath().toAbsolutePath() );
        config.set( GraphDatabaseSettings.transaction_logs_root_path, new File( neo4jHome, "replica-tx-logs-" + serverId ).toPath().toAbsolutePath() );
        memberConfig = config.build();

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
        return boltSocketAddress;
    }

    @Override
    public MemberId id()
    {
        return memberId;
    }

    @Override
    public void start()
    {
        readReplicaGraphDatabase = dbFactory.create( databasesDirectory, memberConfig, newDependencies().monitors( monitors ), discoveryServiceFactory,
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
    public ClientConnectorAddresses clientConnectorAddresses()
    {
        return ClientConnectorAddresses.extractFromConfig( memberConfig );
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
    public String toString()
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
