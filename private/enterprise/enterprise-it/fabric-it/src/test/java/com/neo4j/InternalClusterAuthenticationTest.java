/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.security.AuthManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( FabricEverywhereExtension.class )
class InternalClusterAuthenticationTest
{
    private static DatabaseManagementService dbms;
    private static Path dir;
    private static URI externalBoltUri;
    private static URI internalBoltUri;
    private final List<Driver> usedDrivers = new ArrayList<>();

    @BeforeAll
    static void beforeAll() throws IOException
    {
        var configProperties = Map.of(
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true",
                "dbms.connector.bolt.routing.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.routing.enabled", "true",
                "dbms.security.auth_enabled", "true"
        );

        var config = Config.newBuilder()
                           .setRaw( configProperties )
                           .build();

        dir = Files.createTempDirectory( InternalClusterAuthenticationTest.class.getSimpleName() );

        var dbmsBuilder = new TestDbmsBuilder( dir.toFile() );
        dbms = dbmsBuilder
                .setConfig( config )
                .build();

        var portRegister = dbmsBuilder.dependencies.resolveDependency( ConnectorPortRegister.class );
        var externalBoltAddress = portRegister.getLocalAddress( BoltConnector.NAME );
        var internalBoltAddress = portRegister.getLocalAddress( BoltConnector.INTERNAL_NAME );

        externalBoltUri = getBoltUri( externalBoltAddress );
        internalBoltUri = getBoltUri( internalBoltAddress );

        createUser( "myUser", "reader" );
        runAdminCommand( "CREATE ROLE customRole" );

        var defaultDb = dbms.database( "neo4j" );
        defaultDb.executeTransactionally( "CREATE ()" );
    }

    @AfterEach
    void afterEach()
    {
        usedDrivers.parallelStream().forEach( Driver::close );
    }

    @AfterAll
    static void afterAll() throws IOException
    {
        dbms.shutdown();
        FileUtils.deletePathRecursively( dir );
    }

    @Test
    void testExternalAuthWorksNormally()
    {
        var driver = driver( externalBoltUri, AuthTokens.basic( "myUser", "hello" ) );
        runQuery( driver, 1 );
    }

    @Test
    void testInternalAuthWorks()
    {
        var driver = driver( internalBoltUri, clusterToken( "myUser", List.of( "reader" ) ) );
        runQuery( driver, 1 );
    }

    @Test
    void testInternalTokenCannotBeUsedForExternalAuth()
    {
        var driver = driver( externalBoltUri, clusterToken( "myUser", List.of( "reader" ) ) );
        assertThat( catchThrowableOfType( () -> runQuery( driver, 0 ), AuthenticationException.class ) )
                .hasMessageContaining( "Unsupported authentication token" );
    }

    @Test
    void testInClusterPermissions()
    {
        var driver1 = driver( internalBoltUri, clusterToken( "myUser", List.of() ) );

        assertThat( catchThrowableOfType( () -> runQuery( driver1, 0 ), ClientException.class ) )
                .hasMessageContaining( "Database access is not allowed for user 'myUser' with roles []." );

        var driver2 = driver( internalBoltUri, clusterToken( "myUser", List.of( "customRole", "anotherCustomRole" ) ) );

        assertThat( catchThrowableOfType( () -> runQuery( driver2, 0 ), ClientException.class ) )
                .hasMessageContaining( "Database access is not allowed for user 'myUser' with roles" );

        runAdminCommand( " GRANT ACCESS ON DATABASE * TO customRole" );
        // myUser can access the database now
        runQuery( driver2, 0 );

        runAdminCommand( " GRANT MATCH {*} ON GRAPH * TO customRole" );
        // myUser can read the one node now
        runQuery( driver2, 1 );

        runAdminCommand( " REVOKE MATCH {*} ON GRAPH * FROM customRole" );
        runQuery( driver2, 0 );
    }

    @Test
    void testInvalidInternalToken()
    {
        var driver1 = driver( internalBoltUri, AuthTokens.basic( "myUser", "hello" ) );

        assertThat( catchThrowableOfType( () -> runQuery( driver1, 0 ), AuthenticationException.class ) )
                .hasMessageContaining( "Unsupported authentication token, scheme 'basic' is not supported." );
    }

    private void runQuery( Driver driver, int expectedNodeCount )
    {
        try ( Session session = driver.session() )
        {
            assertEquals( expectedNodeCount, session.run( "MATCH (n) RETURN n" ).list().size() );
        }
    }

    private Driver driver( URI uri, AuthToken authToken )
    {
        var driver = GraphDatabase.driver(
                uri,
                authToken,
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
        usedDrivers.add( driver );
        return driver;
    }

    private static URI getBoltUri( HostnamePort hostPort )
    {
        try
        {
            return new URI( "bolt", null, hostPort.getHost(), hostPort.getPort(), null, null, null );
        }
        catch ( URISyntaxException x )
        {
            throw new IllegalArgumentException( x.getMessage(), x );
        }
    }

    private static void createUser( String username, String role )
    {
        runAdminCommand( "CREATE USER " + username + " SET PASSWORD 'hello' CHANGE NOT REQUIRED" );
        runAdminCommand( "GRANT ROLE " + role + " TO " + username );
    }

    private static void runAdminCommand( String command )
    {
        var systemDb = dbms.database( "system" );
        systemDb.executeTransactionally( command );
    }

    private static class TestDbmsBuilder extends TestEnterpriseDatabaseManagementServiceBuilder
    {
        Dependencies dependencies;

        TestDbmsBuilder( File databaseRootDir )
        {
            super( databaseRootDir );
        }

        @Override
        protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
        {
            return globalModule ->
            {
                dependencies = globalModule.getGlobalDependencies();
                return new TestEditionModule( globalModule );
            };
        }
    }

    private static class TestEditionModule extends EnterpriseEditionModule
    {

        final Dependencies dependencies;

        TestEditionModule( GlobalModule globalModule )
        {
            super( globalModule );

            dependencies = globalModule.getGlobalDependencies();
        }

        @Override
        public AuthManager getBoltInClusterAuthManager()
        {
            var externalAuthManger = getBoltAuthManager( dependencies );

            if ( externalAuthManger == AuthManager.NO_AUTH )
            {
                return AuthManager.NO_AUTH;
            }

            return ((EnterpriseSecurityModule) securityProvider).getInClusterAuthManager();
        }
    }

    private AuthToken clusterToken( String username, List<String> roles )
    {
        return AuthTokens.custom( username, "", null, "in-cluster-token", Map.of( "roles", roles ) );
    }
}
