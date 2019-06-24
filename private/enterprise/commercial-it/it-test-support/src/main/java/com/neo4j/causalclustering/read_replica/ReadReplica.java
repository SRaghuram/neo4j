/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.read_replica;

import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.CatchupPollingProcess;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;
import org.neo4j.monitoring.Monitors;

import static java.util.stream.Collectors.joining;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.helpers.SocketAddress.format;
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
    private final String boltSocketAddress;
    private final Config memberConfig;
    private ReadReplicaGraphDatabase readReplicaGraphDatabase;
    private GraphDatabaseFacade defaultDatabase;
    private GraphDatabaseFacade systemDatabase;
    private DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private final Monitors monitors;
    private final ThreadGroup threadGroup;
    private final File databasesDirectory;
    private final DatabaseIdRepository databaseIdRepository;
    private final ReadReplicaGraphDatabaseFactory dbFactory;
    private volatile boolean hasPanicked;

    public ReadReplica( File parentDir, int serverId, int boltPort, int httpPort, int txPort, int backupPort,
            int discoveryPort, DiscoveryServiceFactory discoveryServiceFactory,
            List<SocketAddress> coreMemberDiscoveryAddresses, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
            String advertisedAddress, String listenAddress, ReadReplicaGraphDatabaseFactory dbFactory )
    {
        this.serverId = serverId;
        this.memberId = new MemberId( UUID.randomUUID() );

        String initialHosts = coreMemberDiscoveryAddresses.stream().map( SocketAddress::toString )
                .collect( joining( "," ) );
        boltSocketAddress = format( advertisedAddress, boltPort );

        Map<String,String> config = stringMap();
        config.put( CommercialEditionSettings.mode.name(), CommercialEditionSettings.Mode.READ_REPLICA.toString() );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialHosts );
        config.put( CausalClusteringSettings.discovery_listen_address.name(), format( listenAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.discovery_advertised_address.name(), format( advertisedAddress, discoveryPort ) );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String,IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        config.put( BoltConnector.group( "bolt" ).enabled.name(), TRUE );
        config.put( BoltConnector.group( "bolt" ).listen_address.name(), format( listenAddress, boltPort ) );
        config.put( BoltConnector.group( "bolt" ).advertised_address.name(), boltSocketAddress );
        config.put( BoltConnector.group( "bolt" ).encryption_level.name(), DISABLED.name() );
        config.put( HttpConnector.group( "http" ).enabled.name(), TRUE );
        config.put( HttpConnector.group( "http" ).listen_address.name(), format( listenAddress, httpPort ) );
        config.put( HttpConnector.group( "http" ).advertised_address.name(), format( advertisedAddress, httpPort ) );

        this.neo4jHome = new File( parentDir, "read-replica-" + serverId );
        config.put( GraphDatabaseSettings.neo4j_home.name(), neo4jHome.getAbsolutePath() );

        config.put( CausalClusteringSettings.transaction_listen_address.name(), format( listenAddress, txPort ) );
        config.put( OnlineBackupSettings.online_backup_listen_address.name(), format( listenAddress, backupPort ) );
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
        this.databaseIdRepository = new PlaceholderDatabaseIdRepository( memberConfig );
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
        readReplicaGraphDatabase =  dbFactory.create( databasesDirectory, memberConfig,
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory, memberId );
        defaultDatabase = (GraphDatabaseFacade) readReplicaGraphDatabase.getManagementService().database( DEFAULT_DATABASE_NAME );
        systemDatabase = (GraphDatabaseFacade) readReplicaGraphDatabase.getManagementService().database( SYSTEM_DATABASE_NAME );
        DependencyResolver dependencyResolver = systemDatabase.getDependencyResolver();
        PanicService panicService = dependencyResolver.resolveDependency( PanicService.class );
        panicService.addPanicEventHandler( () -> hasPanicked = true );

        //noinspection unchecked
        databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
    }

    @Override
    public void shutdown()
    {
        if ( readReplicaGraphDatabase != null )
        {
            try
            {
                readReplicaGraphDatabase.getManagementService().shutdown();
            }
            finally
            {
                readReplicaGraphDatabase = null;
            }
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
    public boolean hasPanicked()
    {
        return hasPanicked;
    }

    public CatchupPollingProcess txPollingClient()
    {
        return defaultDatabase.getDependencyResolver().resolveDependency( CatchupPollingProcess.class );
    }

    public DatabaseManager<ClusteredDatabaseContext> databaseManager()
    {
        return databaseManager;
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
        return databaseManager.getDatabaseContext( databaseIdRepository.get( databaseName ) )
                .map( DatabaseContext::databaseFacade )
                .orElseThrow( DatabaseNotFoundException::new );
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
