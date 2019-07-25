/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;

public class CausalClusterInProcessBuilder
{

    public static WithServerBuilder init()
    {
        return new Builder();
    }

    /**
     * Step Builder to ensure that Cluster has all the required pieces
     * TODO: Add mapping methods to allow for core hosts and replicas to be unevenly distributed between databases
     */
    public static class Builder implements WithServerBuilder, WithCores, WithReplicas, WithLogger, WithPath, WithOptionalPorts
    {

        private BiFunction<File,String,CommercialInProcessNeo4jBuilder> serverBuilder;
        private int numCoreHosts;
        private int numReadReplicas;
        private Log log;
        private Path path;
        private PortPickingFactory portFactory = PortPickingFactory.DEFAULT;
        private final Map<Setting<Object>, Object> config = new HashMap<>();

        @Override
        public WithCores withBuilder( BiFunction<File,String,CommercialInProcessNeo4jBuilder> serverBuilder )
        {
            this.serverBuilder = serverBuilder;
            return this;
        }

        @Override
        public WithReplicas withCores( int n )
        {
            numCoreHosts = n;
            return this;
        }

        @Override
        public WithLogger withReplicas( int n )
        {
            numReadReplicas = n;
            return this;
        }

        @Override
        public WithPath withLogger( LogProvider l )
        {
            log = l.getLog( "org.neo4j.harness.CausalCluster" );
            return this;
        }

        public <T> Builder withConfig( Setting<T> setting, T value )
        {
            config.put( (Setting<Object>) setting, value );
            return this;
        }

        @Override
        public Builder atPath( Path p )
        {
            path = p;
            return this;
        }

        @Override
        public Builder withOptionalPortsStrategy( PortPickingStrategy s )
        {
            portFactory = new PortPickingFactory( s );
            return this;
        }

        public CausalCluster build()
        {
            return new CausalCluster( this );
        }
    }

    /*
     * Builder step interfaces
     */
    public interface WithServerBuilder
    {
        WithCores withBuilder( BiFunction<File,String,CommercialInProcessNeo4jBuilder> serverBuilder );
    }

    public interface WithCores
    {
        WithReplicas withCores( int n );
    }

    public interface WithReplicas
    {
        WithLogger withReplicas( int n );
    }

    public interface WithLogger
    {
        WithPath withLogger( LogProvider l );
    }

    public interface WithPath
    {
        Builder atPath( Path p );
    }

    public interface WithOptionalPorts
    {
        Builder withOptionalPortsStrategy( PortPickingStrategy s );
    }

    /**
     * Port picker functional interface
     */
    public interface PortPickingStrategy
    {
        int port( int offset, int id );
    }

    /**
     * Port picker factory
     */
    public static final class PortPickingFactory
    {
        public static final PortPickingFactory DEFAULT = new PortPickingFactory( ( offset, id ) -> offset + id );

        private final PortPickingStrategy st;

        public PortPickingFactory( PortPickingStrategy st )
        {
            this.st = st;
        }

        int discoveryCorePort( int coreId )
        {
            return st.port( 55000, coreId );
        }

        int txCorePort( int coreId )
        {
            return st.port( 56000, coreId );
        }

        int raftCorePort( int coreId )
        {
            return st.port( 57000, coreId );
        }

        int boltCorePort( int coreId )
        {
            return st.port( 58000, coreId );
        }

        int httpCorePort( int coreId )
        {
            return st.port( 59000, coreId );
        }

        int httpsCorePort( int coreId )
        {
            return st.port( 60000, coreId );
        }

        int discoveryReadReplicaPort( int replicaId )
        {
            return st.port( 55500, replicaId );
        }

        int txReadReplicaPort( int replicaId )
        {
            return st.port( 56500, replicaId );
        }

        int boltReadReplicaPort( int replicaId )
        {
            return st.port( 58500, replicaId );
        }

        int httpReadReplicaPort( int replicaId )
        {
            return st.port( 59500, replicaId );
        }

        int httpsReadReplicaPort( int replicaId )
        {
            return st.port( 60500, replicaId );
        }
    }

    /**
     * Implementation of in process Cluster
     */
    public static class CausalCluster
    {
        private final int nCores;
        private final int nReplicas;
        private final Path clusterPath;
        private final Log log;
        private final PortPickingFactory portFactory;
        private final Map<Setting<Object>, Object> config;
        private final BiFunction<File,String,CommercialInProcessNeo4jBuilder> serverBuilder;

        private final List<InProcessNeo4j> coreNeo4j = synchronizedList( new ArrayList<>() );
        private final List<InProcessNeo4j> replicaControls = synchronizedList( new ArrayList<>() );

        private CausalCluster( CausalClusterInProcessBuilder.Builder builder )
        {
            this.nCores = builder.numCoreHosts;
            this.nReplicas = builder.numReadReplicas;
            this.clusterPath = builder.path;
            this.log = builder.log;
            this.portFactory = builder.portFactory;
            this.config = builder.config;
            this.serverBuilder = builder.serverBuilder;
        }

        public void boot()
        {
            List<SocketAddress> initialMembers = new ArrayList<>( nCores );

            for ( int coreId = 0; coreId < nCores; coreId++ )
            {
                int discoveryPort = portFactory.discoveryCorePort( coreId );
                initialMembers.add( new SocketAddress( "localhost", discoveryPort ) );
            }

            List<Runnable> coreStartActions = new ArrayList<>();
            for ( int coreId = 0; coreId < nCores; coreId++ )
            {
                int discoveryPort = portFactory.discoveryCorePort( coreId );
                int txPort = portFactory.txCorePort( coreId );
                int raftPort = portFactory.raftCorePort( coreId );
                int boltPort = portFactory.boltCorePort( coreId );
                int httpPort = portFactory.httpCorePort( coreId );
                int httpsPort = portFactory.httpsCorePort( coreId );

                String homeDir = "core-" + coreId;

                CommercialInProcessNeo4jBuilder builder = serverBuilder.apply( clusterPath.toFile(), homeDir );

                Path homePath = Paths.get( clusterPath.toString(), homeDir ).toAbsolutePath();
                builder.withConfig( GraphDatabaseSettings.neo4j_home, homePath );
                builder.withConfig( GraphDatabaseSettings.pagecache_memory, "8m" );

                builder.withConfig( CommercialEditionSettings.mode, CommercialEditionSettings.Mode.CORE );
                builder.withConfig( CausalClusteringSettings.multi_dc_license, true );
                builder.withConfig( CausalClusteringSettings.initial_discovery_members, initialMembers );

                builder.withConfig( CausalClusteringSettings.discovery_listen_address, specifyPortOnly( discoveryPort ) );
                builder.withConfig( CausalClusteringSettings.transaction_listen_address, specifyPortOnly( txPort ) );
                builder.withConfig( CausalClusteringSettings.raft_listen_address, specifyPortOnly( raftPort ) );

                builder.withConfig( CausalClusteringSettings.minimum_core_cluster_size_at_formation, nCores );
                builder.withConfig( CausalClusteringSettings.minimum_core_cluster_size_at_runtime, nCores );
                builder.withConfig( CausalClusteringSettings.server_groups, List.of( "core",  "core" + coreId ) );
                configureConnectors( boltPort, httpPort, httpsPort, builder );

                builder.withConfig( OnlineBackupSettings.online_backup_enabled, false );

                config.forEach( builder::withConfig );

                int finalCoreId = coreId;
                coreStartActions.add( () ->
                {
                    coreNeo4j.add( builder.build() );
                    log.info( "Core " + finalCoreId + " started." );
                } );
            }
            executeAll( "Error starting cores", "core-start", coreStartActions );

            List<Runnable> replicaStartActions = new ArrayList<>();
            for ( int replicaId = 0; replicaId < nReplicas; replicaId++ )
            {
                int discoveryPort = portFactory.discoveryReadReplicaPort( replicaId );
                int txPort = portFactory.txReadReplicaPort( replicaId );
                int boltPort = portFactory.boltReadReplicaPort( replicaId );
                int httpPort = portFactory.httpReadReplicaPort( replicaId );
                int httpsPort = portFactory.httpsReadReplicaPort( replicaId );

                String homeDir = "replica-" + replicaId;
                CommercialInProcessNeo4jBuilder builder = serverBuilder.apply( clusterPath.toFile(), homeDir );

                Path homePath = Paths.get( clusterPath.toString(), homeDir ).toAbsolutePath();
                builder.withConfig( GraphDatabaseSettings.neo4j_home, homePath );
                builder.withConfig( GraphDatabaseSettings.pagecache_memory, "8m" );

                builder.withConfig( CommercialEditionSettings.mode, CommercialEditionSettings.Mode.READ_REPLICA );
                builder.withConfig( CausalClusteringSettings.initial_discovery_members, initialMembers );
                builder.withConfig( CausalClusteringSettings.discovery_listen_address, specifyPortOnly( discoveryPort ) );
                builder.withConfig( CausalClusteringSettings.transaction_listen_address, specifyPortOnly( txPort ) );

                builder.withConfig( CausalClusteringSettings.server_groups, List.of( "replica", "replica" + replicaId ) );
                configureConnectors( boltPort, httpPort, httpsPort, builder );

                builder.withConfig( OnlineBackupSettings.online_backup_enabled, false );

                config.forEach( builder::withConfig );

                int finalReplicaId = replicaId;
                replicaStartActions.add( () ->
                {
                    replicaControls.add( builder.build() );
                    log.info( "Read replica " + finalReplicaId + " started." );
                } );
            }
            executeAll( "Error starting read replicas", "replica-start", replicaStartActions );
        }

        private static SocketAddress specifyPortOnly( int port )
        {
            return new SocketAddress( port );
        }

        private static void configureConnectors( int boltPort, int httpPort, int httpsPort, Neo4jBuilder builder )
        {
            builder.withConfig( BoltConnector.enabled, true );
            builder.withConfig( BoltConnector.listen_address, specifyPortOnly( boltPort ) );
            builder.withConfig( BoltConnector.advertised_address, specifyPortOnly( boltPort ) );

            builder.withConfig( HttpConnector.enabled, true );
            builder.withConfig( HttpConnector.listen_address, specifyPortOnly( httpPort ) );
            builder.withConfig( HttpConnector.advertised_address, specifyPortOnly( httpPort ) );

            builder.withConfig( HttpsConnector.enabled, true );
            builder.withConfig( HttpsConnector.listen_address, specifyPortOnly( httpsPort ) );
            builder.withConfig( HttpsConnector.advertised_address, specifyPortOnly( httpsPort ) );
        }

        public List<InProcessNeo4j> getCoreNeo4j()
        {
            return coreNeo4j;
        }

        public List<InProcessNeo4j> getReplicaControls()
        {
            return replicaControls;
        }

        public void shutdown()
        {
            var shutdownActions = Stream.concat( coreNeo4j.stream(), replicaControls.stream() )
                    .map( control -> (Runnable) control::close )
                    .collect( toList() );

            executeAll( "Error shutting down the cluster", "cluster-shutdown", shutdownActions );
        }

        private static void executeAll( String description, String threadPrefix, List<Runnable> actions )
        {
            if ( actions.isEmpty() )
            {
                return;
            }

            var executor = Executors.newFixedThreadPool( actions.size(), daemon( threadPrefix ) );

            try ( var errorHandler = new ErrorHandler( description ) )
            {
                var futures = actions.stream()
                        .map( action -> CompletableFuture.runAsync( action, executor ) )
                        .collect( toList() );

                for ( var future : futures )
                {
                    errorHandler.execute( future::join );
                }
            }
            finally
            {
                executor.shutdown();
            }
        }
    }
}
