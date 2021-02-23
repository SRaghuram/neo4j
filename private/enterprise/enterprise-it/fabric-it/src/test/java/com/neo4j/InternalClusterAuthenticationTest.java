/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.configuration.FabricEnterpriseConfig;
import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.fabric.auth.ClusterCredentialsProvider;
import com.neo4j.fabric.driver.ClusterDriverConfigFactory;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.fabric.executor.ExecutionOptions;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.values.virtual.MapValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InternalClusterAuthenticationTest
{
    private static DatabaseManagementService dbms;
    private static Path dir;
    private static URI externalBoltUri;
    private static URI internalBoltUri;
    private static SocketAddress internalSocketAddress;
    private final List<Driver> usedDrivers = new ArrayList<>();
    private DriverPool driverPool;

    @BeforeAll
    static void beforeAll() throws IOException
    {
        var configProperties = Map.of(
                "dbms.routing.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true",
                "dbms.routing.listen_address", "0.0.0.0:0",
                "dbms.routing.enabled", "true",
                "dbms.security.auth_enabled", "true"
        );

        var config = Config.newBuilder()
                           .setRaw( configProperties )
                           .build();

        dir = Files.createTempDirectory( InternalClusterAuthenticationTest.class.getSimpleName() );

        var dbmsBuilder = new TestDbmsBuilder( dir );
        dbms = dbmsBuilder
                .setConfig( config )
                .build();

        var portRegister = dbmsBuilder.dependencies.resolveDependency( ConnectorPortRegister.class );
        var externalBoltAddress = portRegister.getLocalAddress( BoltConnector.NAME );
        var internalBoltAddress = portRegister.getLocalAddress( BoltConnector.INTERNAL_NAME );

        externalBoltUri = getBoltUri( externalBoltAddress );
        internalBoltUri = getBoltUri( internalBoltAddress );
        internalSocketAddress = new SocketAddress( internalBoltAddress.getHost(), internalBoltAddress.getPort() );

        createUser( "myUser", "reader" );

        var defaultDb = dbms.database( "neo4j" );
        defaultDb.executeTransactionally( "CREATE ()" );
    }

    @BeforeEach
    void beforeEach()
    {
        var config = Config.newBuilder()
                           .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var sslPolicyLoader = SslPolicyLoader.create( config, NullLogProvider.getInstance() );

        var driverConfigFactory = new ClusterDriverConfigFactory( fabricConfig, config, sslPolicyLoader );
        driverPool = new DriverPool( mock( JobScheduler.class ), driverConfigFactory, fabricConfig, Clock.systemUTC(), new ClusterCredentialsProvider() );

        runAdminCommand( "CREATE ROLE customRole" );
    }

    @AfterEach
    void afterEach()
    {
        runAdminCommand( "DROP ROLE customRole" );
        usedDrivers.parallelStream().forEach( Driver::close );
        driverPool.stop();
    }

    @AfterAll
    static void afterAll() throws IOException
    {
        dbms.shutdown();
        FileUtils.deleteDirectory( dir );
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

        runAdminCommand( "GRANT ACCESS ON DATABASE * TO customRole" );
        // myUser can access the database now
        runQuery( driver2, 0 );

        runAdminCommand( "GRANT MATCH {*} ON GRAPH * TO customRole" );
        // myUser can read the one node now
        runQuery( driver2, 1 );

        runAdminCommand( "REVOKE MATCH {*} ON GRAPH * FROM customRole" );
        runQuery( driver2, 0 );
    }

    @Test
    void testInvalidInternalToken()
    {
        var driver1 = driver( internalBoltUri, AuthTokens.basic( "myUser", "hello" ) );

        assertThat( catchThrowableOfType( () -> runQuery( driver1, 0 ), AuthenticationException.class ) )
                .hasMessageContaining( "Unsupported authentication token, scheme 'basic' is not supported." );
    }

    @Test
    void testInternalAuthWorksWithDriverPool()
    {
        var location = getLocation();
        var loginContext = new TestLoginContext( "myUser", Set.of( "reader" ) );
        var pooledDriver = driverPool.getDriver( location, loginContext );

        runQuery( pooledDriver, location, 1 );
    }

    @Test
    void testDriverPoolRolesChangeHandling()
    {
        var location = getLocation();

        var loginContext1 = new TestLoginContext( "myUser", Set.of() );

        var pooledDriver1 = driverPool.getDriver( location, loginContext1 );

        assertThat( catchThrowableOfType( () -> runQuery( pooledDriver1, location, 0 ), FabricException.class ) )
                .hasMessageContaining( "Database access is not allowed for user 'myUser' with roles []." );

        var loginContext2 = new TestLoginContext( "myUser", Set.of( "reader" ) );
        var pooledDriver2 = driverPool.getDriver( location, loginContext2 );
        assertNotSame( pooledDriver1, pooledDriver2 );

        runQuery( pooledDriver2, location, 1 );
    }

    private Location.Remote.Internal getLocation()
    {
        var uri = new Location.RemoteUri( "bolt", List.of( internalSocketAddress ), null );
        return new Location.Remote.Internal( 1, null, uri, null );
    }

    private void runQuery( Driver driver, int expectedNodeCount )
    {
        try ( Session session = driver.session() )
        {
            assertEquals( expectedNodeCount, session.run( "MATCH (n) RETURN n" ).list().size() );
        }
    }

    private void runQuery( PooledDriver driver, Location.Remote location, int expectedNodeCount )
    {
        var txInfo = mock( FabricTransactionInfo.class );
        when( txInfo.getTxTimeout() ).thenReturn( Duration.ZERO );
        var tx = driver.beginTransaction( location, new ExecutionOptions(), AccessMode.READ, txInfo, List.of() ).block();
        var records = tx.run( "MATCH (n) RETURN n", MapValue.EMPTY ).records().collectList().block();
        assertEquals( expectedNodeCount, records.size() );
        tx.commit().block();
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

        TestDbmsBuilder( Path databaseRootDir )
        {
            super( databaseRootDir );
        }

        @Override
        protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
        {
            return globalModule ->
            {
                dependencies = globalModule.getGlobalDependencies();
                return new EnterpriseEditionModule( globalModule );
            };
        }
    }

    private AuthToken clusterToken( String username, List<String> roles )
    {
        return AuthTokens.custom( username, "", null, "in-cluster-token", Map.of( "roles", roles ) );
    }

    private static class TestLoginContext implements EnterpriseLoginContext
    {

        private final String username;
        private final Set<String> roles;

        TestLoginContext( String username, Set<String> roles )
        {
            this.username = username;
            this.roles = roles;
        }

        @Override
        public Set<String> roles()
        {
            return roles;
        }

        @Override
        public AuthSubject subject()
        {
            return new AuthSubject()
            {

                @Override
                public AuthenticationResult getAuthenticationResult()
                {
                    return null;
                }

                @Override
                public void setPasswordChangeNoLongerRequired()
                {

                }

                @Override
                public boolean hasUsername( String username )
                {
                    return false;
                }

                @Override
                public String username()
                {
                    return username;
                }
            };
        }

        @Override
        public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
        {
            return null;
        }
    }
}
