/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.embedded;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.dbms.api.ClusterDatabaseManagementService;
import com.neo4j.dbms.api.ClusterDatabaseManagementServiceBuilder;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static java.time.Duration.ofMinutes;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.CORE;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.READ_REPLICA;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.ports.PortAuthority.allocatePort;
import static org.neo4j.util.concurrent.Futures.getAll;
import static org.neo4j.util.concurrent.Futures.getAllResults;

@TestDirectoryExtension
class EmbeddedClusterIT
{
    @Inject
    private TestDirectory testDirectory;

    private Cluster cluster;

    @BeforeEach
    void before() throws Exception
    {
        cluster = new Cluster( testDirectory.homeDir() );
        cluster.start();

        assertCanReplicate();
    }

    @AfterEach
    void after() throws Exception
    {
        cluster.shutdown();
    }

    @Test
    void shouldHaveNoWritersWhenMajorityShutdown() throws Exception
    {
        assertEventualIsWritableCount( SYSTEM_DATABASE_NAME, 1 );
        assertEventualIsWritableCount( DEFAULT_DATABASE_NAME, 1 );

        var coreA = retry( ofMinutes( 1 ), () -> cluster.getWriter( DEFAULT_DATABASE_NAME ).orElseThrow() );
        coreA.shutdown();

        assertEventualIsWritableCount( SYSTEM_DATABASE_NAME, 1 );
        assertEventualIsWritableCount( DEFAULT_DATABASE_NAME, 1 );

        var coreB = retry( ofMinutes( 1 ), () -> cluster
                .getWriter( DEFAULT_DATABASE_NAME )
                .filter( core -> !core.equals( coreA ) )
                .orElseThrow() );
        coreB.shutdown();

        assertEventualIsWritableCount( SYSTEM_DATABASE_NAME, 0 );
        assertEventualIsWritableCount( DEFAULT_DATABASE_NAME, 0 );
    }

    @Test
    void shouldHaveNoWritersWhenStopped() throws Exception
    {
        assertEventualIsWritableCount( SYSTEM_DATABASE_NAME, 1 );
        assertEventualIsWritableCount( DEFAULT_DATABASE_NAME, 1 );

        retry( ofMinutes( 1 ), () -> cluster
                .getWriter( SYSTEM_DATABASE_NAME ).orElseThrow()
                .shutdownDatabase( DEFAULT_DATABASE_NAME ) );

        assertEventualIsWritableCount( SYSTEM_DATABASE_NAME, 1 );
        assertEventualIsWritableCount( DEFAULT_DATABASE_NAME, 0 );
    }

    private void assertCanReplicate()
    {
        assertDoesNotThrow( () -> retry( ofMinutes( 1 ), () -> write( cluster, DEFAULT_DATABASE_NAME, tx ->
        {
            Node node = tx.createNode( label( "L" ) );
            node.setProperty( "K", "V" );
        } ) ) );

        for ( DatabaseManagementService dbms : cluster.allInstances() )
        {
            var db = dbms.database( DEFAULT_DATABASE_NAME );
            assertDoesNotThrow( () -> retry( ofMinutes( 1 ), () -> read( db, tx -> tx.findNode( label( "L" ), "K", "V" ) ) ) );
        }
    }

    private void assertEventualIsWritableCount( String databaseName, int nWriters )
    {
        assertEventually( () -> cluster
                        .allInstances()
                        .stream()
                        .filter( instance -> isWritableSafe( databaseName, instance ) )
                        .count(),
                count -> count == nWriters,
                1, MINUTES );
    }

    private boolean isWritableSafe( String databaseName, ClusterDatabaseManagementService instance )
    {
        try
        {
            return instance.isWritable( databaseName );
        }
        catch ( DatabaseNotFoundException e )
        {
            return false;
        }
    }

    private void retry( Duration timeout, ThrowingAction<Exception> action ) throws Exception
    {
        retry( timeout, () ->
        {
            action.apply();
            return action;
        } );
    }

    private <R> R retry( Duration timeout, ThrowingSupplier<R,Exception> supplier ) throws Exception
    {
        var end = Clocks.nanoClock().instant().plus( timeout );
        do
        {
            try
            {
                R value = supplier.get();
                if ( value == null )
                {
                    throw new NullPointerException();
                }
                return value;
            }
            catch ( Exception ex )
            {
                if ( Clocks.nanoClock().instant().isAfter( end ) )
                {
                    throw ex;
                }
                Thread.sleep( 1000 );
            }
        }
        while ( true );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void write( Cluster cluster, String databaseName, Consumer<Transaction> writer )
    {
        var db = cluster
                .getWriter( databaseName )
                .map( c -> c.database( databaseName ) )
                .orElseThrow( () -> new IllegalStateException( "No leader found" ) );

        write( db, writer );
    }

    private void write( GraphDatabaseService db, Consumer<Transaction> writer )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            writer.accept( transaction );
            transaction.commit();
        }
    }

    private <R> R read( GraphDatabaseService db, Function<Transaction,R> reader )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return reader.apply( transaction );
        }
    }

    private static class Cluster
    {
        private static final int N_CORES = 3;
        private static final int N_REPLICAS = 3;

        private final File baseDir;

        private List<ClusterDatabaseManagementService> cores;
        private List<ClusterDatabaseManagementService> readReplicas;

        private Cluster( File baseDir )
        {
            this.baseDir = baseDir;
        }

        void start() throws Exception
        {
            var coreDiscoveryAddresses = range( 0, N_CORES )
                    .mapToObj( ignored -> allocatePort() )
                    .map( port -> new SocketAddress( "localhost", port ) )
                    .collect( toList() );

            var fCores = range( 0, N_CORES )
                    .mapToObj( serverId -> supplyAsync( () -> startCore( serverId, coreDiscoveryAddresses ) ) )
                    .collect( toList() );

            cores = getAllResults( fCores );

            var fReadReplicas = range( 0, N_REPLICAS )
                    .mapToObj( serverId -> supplyAsync( () -> startReadReplica( serverId, coreDiscoveryAddresses ) ) )
                    .collect( toList() );

            readReplicas = getAllResults( fReadReplicas );
        }

        void shutdown() throws ExecutionException
        {
            getAll( readReplicas.stream().map( dbms -> runAsync( dbms::shutdown ) ).collect( toList() ) );
            getAll( cores.stream().map( dbms -> runAsync( dbms::shutdown ) ).collect( toList() ) );
        }

        Optional<ClusterDatabaseManagementService> getWriter( String databaseName )
        {
            for ( ClusterDatabaseManagementService core : cores )
            {
                boolean isWritable;
                try
                {
                    isWritable = core.isWritable( databaseName );
                }
                catch ( DatabaseNotFoundException e )
                {
                    continue;
                }
                if ( isWritable )
                {
                    return Optional.of( core );
                }
            }
            return Optional.empty();
        }

        Set<ClusterDatabaseManagementService> allInstances()
        {
            var allNodes = new HashSet<ClusterDatabaseManagementService>( cores.size() + readReplicas.size() );
            allNodes.addAll( cores );
            allNodes.addAll( readReplicas );
            return allNodes;
        }

        ClusterDatabaseManagementService startCore( int serverId, List<SocketAddress> coreDiscoveryAddresses )
        {
            var homeDir = new File( baseDir, "core-" + serverId );

            var builder = new ClusterDatabaseManagementServiceBuilder( homeDir );

            builder.setConfig( GraphDatabaseSettings.mode, CORE );

            builder.setConfig( CausalClusteringSettings.initial_discovery_members, coreDiscoveryAddresses );

            builder.setConfig( CausalClusteringSettings.discovery_listen_address, coreDiscoveryAddresses.get( serverId ) );
            builder.setConfig( CausalClusteringSettings.discovery_advertised_address, coreDiscoveryAddresses.get( serverId ) );

            var txAddress = new SocketAddress( "localhost", allocatePort() );
            builder.setConfig( CausalClusteringSettings.transaction_listen_address, txAddress );
            builder.setConfig( CausalClusteringSettings.transaction_advertised_address, txAddress );

            var raftAddress = new SocketAddress( "localhost", allocatePort() );
            builder.setConfig( CausalClusteringSettings.raft_listen_address, raftAddress );
            builder.setConfig( CausalClusteringSettings.raft_advertised_address, raftAddress );

            var backupAddress = new SocketAddress( "localhost", allocatePort() );
            builder.setConfig( OnlineBackupSettings.online_backup_listen_address, backupAddress );

            return builder.build();
        }

        ClusterDatabaseManagementService startReadReplica( int serverId, List<SocketAddress> coreDiscoveryAddresses )
        {
            var homeDir = new File( baseDir, "read-replica-" + serverId );

            var builder = new ClusterDatabaseManagementServiceBuilder( homeDir );

            builder.setConfig( GraphDatabaseSettings.mode, READ_REPLICA );

            builder.setConfig( CausalClusteringSettings.initial_discovery_members, coreDiscoveryAddresses );

            var discoveryAddress = new SocketAddress( "localhost", allocatePort() );
            builder.setConfig( CausalClusteringSettings.discovery_listen_address, discoveryAddress );
            builder.setConfig( CausalClusteringSettings.discovery_advertised_address, discoveryAddress );

            var txAddress = new SocketAddress( "localhost", allocatePort() );
            builder.setConfig( CausalClusteringSettings.transaction_listen_address, txAddress );
            builder.setConfig( CausalClusteringSettings.transaction_advertised_address, txAddress );

            var backupAddress = new SocketAddress( "localhost", allocatePort() );
            builder.setConfig( OnlineBackupSettings.online_backup_listen_address, backupAddress );

            return builder.build();
        }
    }
}
