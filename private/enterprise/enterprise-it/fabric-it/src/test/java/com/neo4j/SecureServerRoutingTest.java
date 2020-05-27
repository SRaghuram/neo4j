/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Level;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.connectors.BoltConnector.connector_routing_enabled;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@TestDirectoryExtension
@ExtendWith( {FabricEverywhereExtension.class,
        DefaultFileSystemExtension.class,
        SuppressOutputExtension.class,
        MockedRoutingContextExtension.class} )
@ClusterExtension
class SecureServerRoutingTest extends ClusterTestSupport
{

    private static final String CERTIFICATES_DIR = "certificates/cluster";

    @Inject
    private static FileSystemAbstraction fs;

    @Inject
    protected static RoutingContext routingContext;

    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    private static Map<Integer,Driver> coreDrivers = new HashMap<>();
    private static Set<Driver> drivers = new HashSet<>();

    private static Driver fooFollowerDriver;

    private static Driver readReplicaDriver;

    private static Driver adminDriver;

    private static List<Bookmark> bookmarks = new ArrayList<>();

    @BeforeAll
    static void beforeAll() throws Exception
    {
        var sslPolicyConfig = SslPolicyConfig.forScope( CLUSTER );

        var coreParams = Map.of(
                CausalClusteringSettings.middleware_logging_level.name(), Level.DEBUG.toString(),
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                sslPolicyConfig.enabled.name(), TRUE,
                sslPolicyConfig.base_directory.name(), CERTIFICATES_DIR,
                connector_routing_enabled.name(), "true"
        );
        var readReplicaParams = Map.of(
                CausalClusteringSettings.middleware_logging_level.name(), Level.DEBUG.toString(),
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                sslPolicyConfig.enabled.name(), TRUE,
                sslPolicyConfig.base_directory.name(), CERTIFICATES_DIR,
                connector_routing_enabled.name(), "true"
        );

        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                   .withNumberOfCoreMembers( 2 )
                                                   .withNumberOfReadReplicas( 1 )
                                                   .withSharedCoreParams( coreParams )
                                                   .withSharedReadReplicaParams( readReplicaParams );

        cluster = clusterFactory.createCluster( clusterConfig );

        // install the cryptographic objects for each core
        for ( var core : cluster.coreMembers() )
        {
            var keyId = core.serverId();
            var homeDir = cluster.getCoreMemberById( core.serverId() ).homeDir();
            installKeyToInstance( homeDir, keyId );
        }

        // install the cryptographic objects for each read replica
        for ( var replica : cluster.readReplicas() )
        {
            var keyId = replica.serverId() + cluster.coreMembers().size();
            var homeDir = cluster.getReadReplicaById( replica.serverId() ).homeDir();
            installKeyToInstance( homeDir, keyId );
        }

        cluster.start();

        var systemLeader = cluster.awaitLeader( "system" );
        var driverWithDefaultNeo4jPassword = driver( systemLeader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
        runAdminCommand( driverWithDefaultNeo4jPassword, "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '1234'" );

        adminDriver = driver( systemLeader.directURI(), AuthTokens.basic( "neo4j", "1234" ) );
        runAdminCommand( "CREATE USER myUser SET PASSWORD 'hello' CHANGE NOT REQUIRED" );
        runAdminCommand( "CREATE DATABASE foo" );

        var fooLeader = cluster.awaitLeader( "foo" );
        awaitDbAvailable( cluster, "foo" );

        var readReplica = cluster.findAnyReadReplica();
        readReplicaDriver = driver( readReplica.directURI() );

        cluster.coreMembers().forEach( core -> coreDrivers.put( core.serverId(), driver( core.directURI() ) ) );

        var fooFollower = getFollower( cluster, fooLeader );
        fooFollowerDriver = coreDrivers.get( fooFollower.serverId() );
    }

    @BeforeEach
    void beforeEach() throws TimeoutException
    {
        // some tests revoke admin role from myUser,
        // so it is granted before each test
        runAdminCommand( "GRANT ROLE admin TO myUser" );

        runAdminCommand( "CREATE ROLE myRole" );
        runAdminCommand( "GRANT ROLE myRole TO myUser" );
        super.beforeEach( routingContext, cluster, new ArrayList<>( coreDrivers.values() ), readReplicaDriver );
    }

    @AfterEach
    void afterEach()
    {
        // some tests modify the privileges of this role
        // the easiest clean up is to drop it
        runAdminCommand( "DROP ROLE myRole" );
    }

    @AfterAll
    static void afterAll()
    {
        Stream.<Runnable>concat(
                drivers.stream().map( d -> d::close ),
                Stream.of( cluster::shutdown )
        ).parallel()
         .forEach( Runnable::run );
    }

    @Test
    void testPlainTextInServer2Server() throws TimeoutException, IOException
    {
        var fooLeader = cluster.awaitLeader( "foo" );
        var address = fooLeader.intraClusterBoltAdvertisedAddress();

        var addressParts = address.split( ":" );
        var socketAddress = new InetSocketAddress( addressParts[0], Integer.parseInt( addressParts[1] ) );
        var socket = new Socket();

        // Let's check that the server is listening on the address ...
        socket.connect( socketAddress );
        socket.close();

        // ... but a 'plain text' driver can't talk to it
        var uri = String.format( "bolt://%s", address );
        try ( var driver = driver( uri, AuthTokens.basic( "username", "password" ) ) )
        {
            try ( var session = driver.session() )
            {
                assertThatExceptionOfType( ServiceUnavailableException.class )
                        .isThrownBy( () -> session.run( "RETURN 1" ).list() )
                        .withMessageContaining( "Connection to the database terminated." );
            }
        }
    }

    @Test
    void testWriteOnFollower()
    {
        doTestWriteOnClusterMember( fooFollowerDriver );
    }

    @Test
    void testWriteOnReadReplica()
    {
        doTestWriteOnClusterMember( readReplicaDriver );
    }

    @Test
    void testPermissionsThroughFollower()
    {
        doTestPermissions( fooFollowerDriver );
    }

    @Test
    void testPermissionsThroughReadReplica()
    {
        doTestPermissions( readReplicaDriver );
    }

    void doTestPermissions( Driver driver )
    {
        runAdminCommand( "REVOKE ROLE admin FROM myUser" );

        var query = joinAsLines(
                "USE foo",
                "MATCH (n)",
                "SET n.foo = 'bar'",
                "RETURN n" );

        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> runQuery( driver, query, 0 ) )
                .withMessageContaining( "Database access is not allowed for user 'myUser' with roles" );

        runAdminCommand( "GRANT ACCESS ON DATABASE * TO myRole" );
        // myUser can access the database now
        runQuery( driver, query, 0 );

        runAdminCommand( "GRANT MATCH {*} ON GRAPH * TO myRole" );
        runAdminCommand( "GRANT CREATE NEW PROPERTY NAME ON DATABASE * TO myRole" );
        runAdminCommand( "GRANT WRITE ON GRAPH * TO myRole" );
        // myUser can read the 2 nodes in the DB now
        runQuery( driver, query, 2 );

        runAdminCommand( "REVOKE MATCH {*} ON GRAPH * FROM myRole" );
        runQuery( driver, query, 0 );
    }

    private void runQuery( Driver driver, String query, int expectedRecords )
    {
        var records = run( driver, "neo4j", AccessMode.WRITE, session -> session.run( query ).list() );
        assertEquals( expectedRecords, records.size() );
    }

    private static void runAdminCommand( String command )
    {
        runAdminCommand( adminDriver, command );
    }

    private static void runAdminCommand( Driver driver, String command )
    {
        try ( var session = driver.session( SessionConfig.builder()
                                                         .withDatabase( "system" )
                                                         .withDefaultAccessMode( AccessMode.WRITE )
                                                         .withBookmarks( bookmarks ).build() ) )
        {
            session.run( command ).consume();
            bookmarks.add( session.lastBookmark() );
        }
    }

    static Driver driver( String uri, AuthToken authToken )
    {
        var driver = GraphDatabase.driver(
                uri,
                authToken,
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
        drivers.add( driver );
        return driver;
    }

    static Driver driver( String uri )
    {
        return driver( uri, AuthTokens.basic( "myUser", "hello" ) );
    }

    private static void installKeyToInstance( File homeDir, int keyId ) throws IOException
    {
        var baseDir = new File( homeDir, CERTIFICATES_DIR );
        fs.mkdirs( new File( baseDir, "trusted" ) );
        fs.mkdirs( new File( baseDir, "revoked" ) );

        SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( baseDir );
    }
}