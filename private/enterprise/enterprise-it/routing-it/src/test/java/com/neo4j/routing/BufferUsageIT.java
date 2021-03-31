/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.bolt.BoltServer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
class BufferUsageIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private TestDirectory testDirectory;

    private Cluster cluster;
    private Driver coreDriver;
    private Driver replicaDriver;

    // the joy of working with static objects ...
    private Map<Integer,Long> defaultNettyPoolStatsBefore;
    private Map<Integer,Long> boltServerNettyPoolStatsBefore;

    @BeforeEach
    void beforeEach() throws Exception
    {
        defaultNettyPoolStatsBefore = getStats( (PooledByteBufAllocator) ByteBufAllocator.DEFAULT );
        boltServerNettyPoolStatsBefore = getStats( BoltServer.NETTY_BUF_ALLOCATOR );
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                   .withNumberOfCoreMembers( 2 )
                                                   .withSharedPrimaryParam( GraphDatabaseInternalSettings.managed_network_buffers, "true" )
                                                   .withNumberOfReadReplicas( 1 )
                                                   .withSharedReadReplicaParam( GraphDatabaseInternalSettings.managed_network_buffers, "true" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        cluster.awaitLeader();

        CoreClusterMember coreClusterMember = cluster.primaryMembers().stream().findAny().get();
        coreDriver = getDriver( coreClusterMember.routingURI() );
        replicaDriver = getDriver( cluster.findAnyReadReplica().directURI() );
    }

    @AfterEach
    void afterEach()
    {
        Stream.<Runnable>of( cluster::shutdown, coreDriver::close, replicaDriver::close )
                .parallel()
                .forEach( Runnable::run );
    }

    @Test
    void testClusterInteraction() throws IOException
    {
        var writeQuery = "UNWIND range(0, 10) AS i\n"
                + "CREATE (p:Person {id:'id-prefix-i'})\n"
                + "RETURN p";

        Bookmark bookmark;
        try ( var session = coreDriver.session() )
        {
            session.run( writeQuery ).list();
            bookmark = session.lastBookmark();
        }

        // Up to this point, every component potentially allocating Netty's buffers except for the Bolt server on a RR
        // and Back up servers should have been tested.
        try ( var session = replicaDriver.session( SessionConfig.builder().withBookmarks( bookmark ).build() ) )
        {
            session.run( "MATCH (p:Person) RETURN p" ).list();
        }

        // we cannot use the real back-up client because it would use the default static
        // Netty allocator and we would not be able to distinguish if it was used just
        // by the back-up client or it was touched by any server component, too.
        // Therefore we must improvise slightly.
        // Basically, we just need to force the back-up server to allocate a buffer,
        // which it will need to do if anything is sent to its socket.
        pokeCoreBackUpSocket();
        pokeReplicaBackUpSocket();

        assertEquals( defaultNettyPoolStatsBefore, getStats( (PooledByteBufAllocator) ByteBufAllocator.DEFAULT ) );
        assertEquals( boltServerNettyPoolStatsBefore, getStats( BoltServer.NETTY_BUF_ALLOCATOR ) );
    }

    private Map<Integer,Long> getStats( PooledByteBufAllocator allocator )
    {
        var stats = new HashMap<Integer,Long>();

        allocator.metric()
                 .directArenas()
                 .forEach( poolArenaMetric -> stats.put( System.identityHashCode( poolArenaMetric ), poolArenaMetric.numAllocations() ) );
        return stats;
    }

    private void pokeCoreBackUpSocket() throws IOException
    {
        CoreClusterMember coreClusterMember = cluster.primaryMembers().stream().findAny().get();
        int port = backupPort( coreClusterMember.defaultDatabase() );
        pokeBackUpSocket( port );
    }

    private void pokeReplicaBackUpSocket() throws IOException
    {
        int port = backupPort( cluster.findAnyReadReplica().defaultDatabase() );
        pokeBackUpSocket( port );
    }

    private void pokeBackUpSocket( int port ) throws IOException
    {
        try ( var socket = new Socket() )
        {
            socket.connect( new InetSocketAddress( "localhost", port ) );
            // if we come here without an exception, it means we have a connection to the back up server
            var output = socket.getOutputStream();
            output.write( "Hello back-up server, are you there?".getBytes() );
            output.flush();
            // The back-up server will obviously react to the garbage
            // we sent over with closing the connection.
            // However, reading that garbage forced it to allocate a buffer
            // and we can check if it touched the default Netty allocator or not
            // which is what this test cares about.
            assertEquals( -1, socket.getInputStream().read() );
        }
    }

    private int backupPort( GraphDatabaseService db )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( BACKUP_SERVER_NAME );
        assertNotNull( address, "Backup server address not registered" );
        return address.getPort();
    }

    private static Driver getDriver( String uri )
    {
        return GraphDatabase.driver(
                uri,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
    }
}
