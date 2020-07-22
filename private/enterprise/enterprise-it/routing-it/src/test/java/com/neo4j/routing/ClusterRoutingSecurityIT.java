/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.commandline.admin.security.SetOperatorPasswordCommand;
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
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@TestDirectoryExtension
@ExtendWith( {
        DefaultFileSystemExtension.class,
        SuppressOutputExtension.class,
        MockedRoutingContextExtension.class} )
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
class ClusterRoutingSecurityIT extends ClusterTestSupport
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

    private static Driver readReplicaUpgradeDriver;

    private static Driver adminDriver;

    private static List<Driver> adminDrivers;

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
                GraphDatabaseSettings.routing_enabled.name(), TRUE,
                GraphDatabaseInternalSettings.restrict_upgrade.name(), TRUE
        );
        var readReplicaParams = Map.of(
                CausalClusteringSettings.middleware_logging_level.name(), Level.DEBUG.toString(),
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                sslPolicyConfig.enabled.name(), TRUE,
                sslPolicyConfig.base_directory.name(), CERTIFICATES_DIR,
                GraphDatabaseSettings.routing_enabled.name(), TRUE,
                GraphDatabaseInternalSettings.restrict_upgrade.name(), TRUE
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
            var index = core.index();
            var homeDir = cluster.getCoreMemberByIndex( core.index() ).homePath();
            installKeyToInstance( homeDir, index );
            addUpgradeUser( homeDir );
        }

        // install the cryptographic objects for each read replica
        for ( var replica : cluster.readReplicas() )
        {
            var index = replica.index() + cluster.coreMembers().size();
            var homeDir = cluster.getReadReplicaByIndex( replica.index() ).homePath();
            installKeyToInstance( homeDir, index );
            addUpgradeUser( homeDir );
        }

        cluster.start();

        var systemLeader = cluster.awaitLeader( "system" );
        var driverWithDefaultNeo4jPassword = driver( systemLeader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
        runAdminCommand( driverWithDefaultNeo4jPassword, "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '1234'" );

        adminDriver = adminDriver( systemLeader.directURI() );
        runAdminCommand( "CREATE USER myUser SET PASSWORD 'hello' CHANGE NOT REQUIRED" );
        runAdminCommand( "CREATE DATABASE foo" );

        var fooLeader = cluster.awaitLeader( "foo" );
        awaitDbAvailable( cluster, "foo" );

        var readReplica = cluster.findAnyReadReplica();
        readReplicaDriver = driver( readReplica.directURI() );

        cluster.coreMembers().forEach( core -> coreDrivers.put( core.index(), driver( core.directURI() ) ) );

        var fooFollower = getFollower( cluster, fooLeader );
        fooFollowerDriver = coreDrivers.get( fooFollower.index() );

        readReplicaUpgradeDriver = driver( readReplica.directURI(), GraphDatabaseInternalSettings.upgrade_username.defaultValue(), "foo" );

        List<String> allUris = new ArrayList<>();
        cluster.coreMembers().forEach( member -> allUris.add( member.directURI() ) );
        cluster.readReplicas().forEach( member -> allUris.add( member.directURI() ) );

        adminDrivers = allUris.stream()
                              .map( ClusterRoutingSecurityIT::adminDriver )
                              .collect( Collectors.toList() );
    }

    @BeforeEach
    void beforeEach() throws TimeoutException
    {
        // some tests revoke admin role from myUser,
        // so it is granted before each test
        runAdminCommand( "GRANT ROLE admin TO myUser" );

        runAdminCommand( "CREATE ROLE myRole" );
        runAdminCommand( "GRANT ROLE myRole TO myUser" );
        invalidateAuthCaches();
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
                        .isThrownBy( () -> session.writeTransaction( tx -> tx.run( "RETURN 1" ).list() ) )
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

    @Test
    void testAdminWriteProcedureThroughReadReplica()
    {
        runQuery( readReplicaUpgradeDriver, "CALL dbms.upgrade() YIELD status RETURN status", 1, "system" );
    }

    void doTestPermissions( Driver driver )
    {
        runAdminCommand( "REVOKE ROLE admin FROM myUser" );
        invalidateAuthCaches();

        var query = joinAsLines(
                "USE foo",
                "MATCH (n)",
                "SET n.foo = 'bar'",
                "RETURN n" );

        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> runQuery( driver, query, 0 ) )
                .withMessageContaining( "Database access is not allowed for user 'myUser' with roles" );

        runAdminCommand( "GRANT ACCESS ON DATABASE * TO myRole" );
        invalidateAuthCaches();
        // myUser can access the database now
        runQuery( driver, query, 0 );

        runAdminCommand( "GRANT MATCH {*} ON GRAPH * TO myRole" );
        runAdminCommand( "GRANT CREATE NEW PROPERTY NAME ON DATABASE * TO myRole" );
        runAdminCommand( "GRANT WRITE ON GRAPH * TO myRole" );
        invalidateAuthCaches();
        // myUser can read the 2 nodes in the DB now
        runQuery( driver, query, 2 );

        runAdminCommand( "REVOKE MATCH {*} ON GRAPH * FROM myRole" );
        invalidateAuthCaches();
        runQuery( driver, query, 0 );
    }

    private void runQuery( Driver driver, String query, int expectedRecords )
    {
        runQuery( driver, query, expectedRecords, "neo4j" );
    }

    private void runQuery( Driver driver, String query, int expectedRecords, String database )
    {
        var records = run( driver, database, AccessMode.WRITE,
                session -> session.writeTransaction( tx -> tx.run( query ).list() ) );
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
            session.writeTransaction( tx -> tx.run( command ).consume() );
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
        return driver( uri, "myUser", "hello" );
    }

    static Driver driver( String uri, String username, String password )
    {
        return driver( uri, AuthTokens.basic( username, password ) );
    }

    static Driver adminDriver( String uri )
    {
        return driver( uri, AuthTokens.basic( "neo4j", "1234" ) );
    }

    private void invalidateAuthCaches()
    {
        when( routingContext.isServerRoutingEnabled() ).thenReturn( false );
        try
        {
            // Principal propagation during server side routing is quite complex,
            // so better play it safe and invalidate caches in the entire cluster
            // ( As opposed to trying to figure out where the caches need
            // to be invalidated for each scenario. Especially if flakiness
            // is the result of getting it wrong )
            adminDrivers.forEach( driver -> runAdminCommand( driver, "CALL dbms.security.clearAuthCache()" ) );
        }
        finally
        {
            when( routingContext.isServerRoutingEnabled() ).thenReturn( true );
        }
    }

    private static void installKeyToInstance( Path homeDir, int index ) throws IOException
    {
        var baseDir = homeDir.resolve( CERTIFICATES_DIR );
        fs.mkdirs( baseDir.resolve( "trusted" ) );
        fs.mkdirs( baseDir.resolve( "revoked" ) );

        SslResourceBuilder.caSignedKeyId( index ).trustSignedByCA().install( baseDir );
    }

    private static void addUpgradeUser( Path homeDir )
    {
        final var ctx = new ExecutionContext( homeDir, homeDir.resolve( "conf" ), mock( PrintStream.class ), mock( PrintStream.class ), fs );
        final var command = new SetOperatorPasswordCommand( ctx );
        CommandLine.populateCommand( command, "foo" );
        command.execute();
    }
}
