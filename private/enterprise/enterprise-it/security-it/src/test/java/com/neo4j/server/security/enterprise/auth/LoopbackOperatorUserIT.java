/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.commandline.admin.security.SetOperatorPasswordCommand;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.PrintStream;
import java.net.Inet4Address;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.DEFAULT_LOOPBACK_CONNECTOR_PORT;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@ExtendWith( EphemeralFileSystemExtension.class )
class LoopbackOperatorUserIT
{
    private static final String LOOPBACK_URI = "neo4j://127.0.0.1:" + DEFAULT_LOOPBACK_CONNECTOR_PORT;
    private static final org.neo4j.driver.Config config = org.neo4j.driver.Config.builder()
                                                                                 .withLogging( Logging.none() )
                                                                                 .withoutEncryption()
                                                                                 .withConnectionTimeout( 10, TimeUnit.SECONDS )
                                                                                 .build();

    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;
    private DatabaseManagementService enterpriseDbms;

    @BeforeEach
    void setup()
    {
        Path graphDir = Path.of( DEFAULT_DATABASE_NAME ).resolve( UUID.randomUUID().toString() );
        confDir = graphDir.resolve( "conf" );
        homeDir = graphDir.resolve( "home" );
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if ( enterpriseDbms != null )
        {
            enterpriseDbms.shutdown();
        }
        fileSystem.close();
    }

    @Test
    void shouldConnectAsOperatorRestrictionEnabled()
    {
        // GIVEN
        setupRestricted();

        try ( Driver driver = GraphDatabase.driver( LOOPBACK_URI, AuthTokens.basic( "IGNORED", "bar" ), config );
              Session session = driver.session( SessionConfig.builder().withDatabase( DEFAULT_DATABASE_NAME ).build() ) )
        {
            // WHEN
            Record record = session.run( "CALL dbms.showCurrentUser() YIELD username, roles" ).single();
            // THEN
            assertThat( record.get( "username" ).asString() ).isEqualTo( "" );
            assertThat( record.get( "roles" ).asList() ).isEmpty();
        }

        try ( Driver driver = GraphDatabase.driver( LOOPBACK_URI, AuthTokens.basic( "IGNORED", "bar" ), config );
              Session session = driver.session( SessionConfig.builder().withDatabase( SYSTEM_DATABASE_NAME ).build() ) )
        {
            // WHEN
            Record record = session.run( "CALL dbms.showCurrentUser() YIELD username, roles" ).single();
            // THEN
            assertThat( record.get( "username" ).asString() ).isEqualTo( "" );
            assertThat( record.get( "roles" ).asList() ).isEmpty();
        }
    }

    @Test
    void shouldNotConnectExternallyToLoopbackPort() throws Exception
    {
        // GIVEN
        setupRestricted();

        var ip = Inet4Address.getLocalHost().getHostAddress();

        // WHEN
        String uri = "neo4j://" + ip + ":" + DEFAULT_LOOPBACK_CONNECTOR_PORT;
        assertThatThrownBy( () ->
        {
            Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( "IGNORED", "bar" ), config );
            driver.verifyConnectivity();
        } ).isInstanceOf( ServiceUnavailableException.class );
    }

    @Test
    void shouldNotAuthenticateOperatorOnNormalPort()
    {
        // GIVEN
        setupRestricted();
        var ip = Inet4Address.getLoopbackAddress().getHostAddress();
        ConnectorPortRegister portRegister =
                ((GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME )).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort bolt = portRegister.getLocalAddress( "bolt" );

        // WHEN
        String uri = "neo4j://" + ip + ":" + bolt.getPort();
        assertThatThrownBy( () ->
        {
            Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( "IGNORED", "bar" ), config );
            driver.verifyConnectivity();
        } ).isInstanceOf( AuthenticationException.class );
    }

    @Test
    void shouldNotConnectAsOperatorRestrictionDisabled()
    {
        // GIVEN
        setupUnrestricted();
        assertThatThrownBy( () ->
        {
            Driver driver = GraphDatabase.driver( LOOPBACK_URI, AuthTokens.basic( "IGNORED", "bar" ), config );
            driver.verifyConnectivity();
        } ).isInstanceOf( ServiceUnavailableException.class );
    }

    @ParameterizedTest
    @ValueSource( strings = {"dbms.upgrade", "dbms.upgradeStatus", "dbms.upgradeDetails", "dbms.upgradeStatusDetails"} )
    void shouldOnlyAllowOperatorUserToCallUpgradeProcedure( String procedureName )
    {
        String query = String.format( "CALL %s()", procedureName );
        setInitialPassword();
        setupRestricted();
        try ( Session session = loginLoopback() )
        {
            // should not throw
            session.run( query ).consume();
        }

        assertThatThrownBy( () -> {
            Session session = loginBolt();
            session.run( query ).consume();
        } ).hasMessageContaining( "Executing procedure is not allowed for user" );
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToCreateDatabase( boolean blocked )
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        try ( Session session = loginLoopback() )
        {
            session.run( "CREATE DATABASE operational" ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DATABASES" );
            Set<String> names = result.stream().map( r -> r.get( "name" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, "operational" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToCreateDatabaseIfNotExists( boolean blocked )
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        try ( Session session = loginLoopback() )
        {
            // Existing database
            session.run( String.format( "CREATE DATABASE %s IF NOT EXISTS", DEFAULT_DATABASE_NAME ) ).consume();
            // Non-existing database
            session.run( "CREATE DATABASE operational IF NOT EXISTS" ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DATABASES" );
            Set<String> names = result.stream().map( r -> r.get( "name" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, "operational" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToCreateOrReplaceDatabase( boolean blocked )
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        try ( Session session = loginLoopback() )
        {
            // Existing database
            session.run( String.format( "CREATE OR REPLACE DATABASE %s", DEFAULT_DATABASE_NAME ) ).consume();
            // Non-existing database
            session.run( "CREATE OR REPLACE DATABASE operational" ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DATABASES" );
            Set<String> names = result.stream().map( r -> r.get( "name" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, "operational" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToDropDatabase( boolean blocked )
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        try ( Session session = loginLoopback() )
        {
            session.run( String.format( "DROP DATABASE %s", DEFAULT_DATABASE_NAME ) ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DATABASES" );
            Set<String> names = result.stream().map( r -> r.get( "name" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( SYSTEM_DATABASE_NAME );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToDropDatabaseIfExists( boolean blocked )
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        try ( Session session = loginLoopback() )
        {
            // Existing database
            session.run( String.format( "DROP DATABASE %s IF EXISTS", DEFAULT_DATABASE_NAME ) ).consume();
            // Non-existing database
            session.run( "DROP DATABASE operational IF EXISTS" ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DATABASES" );
            Set<String> names = result.stream().map( r -> r.get( "name" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( SYSTEM_DATABASE_NAME );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToStartAndStopDatabase( boolean blocked )
    {
        // GIVEN
        setupRestricted( blocked );

        // WHEN
        try ( Session session = loginLoopback() )
        {
            // Stop online database
            session.run( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DEFAULT DATABASE" );
            Set<String> names = result.stream().map( r -> r.get( "currentStatus" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( "offline" );
        }

        try ( Session session = loginLoopback() )
        {
            // Stop offline database
            session.run( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DEFAULT DATABASE" );
            Set<String> names = result.stream().map( r -> r.get( "currentStatus" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( "offline" );
        }

        // WHEN
        try ( Session session = loginLoopback() )
        {
            // Start offline database
            session.run( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DEFAULT DATABASE" );
            Set<String> names = result.stream().map( r -> r.get( "currentStatus" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( "online" );
        }

        // WHEN
        try ( Session session = loginLoopback() )
        {
            // Start online database
            session.run( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) ).consume();
        }

        // THEN
        try ( Session session = loginLoopback() )
        {
            Result result = session.run( "SHOW DEFAULT DATABASE" );
            Set<String> names = result.stream().map( r -> r.get( "currentStatus" ).asString() ).collect( Collectors.toSet() );
            assertThat( names ).contains( "online" );
        }
    }

    @Test
    void shouldNotListUpgradeUser()
    {
        setInitialPassword();
        setupRestricted();

        try ( Session session = loginBolt() )
        {
            Record user = session.run( "SHOW USERS" ).single();
            assertThat( user.get( "user" ).asString() ).isEqualTo( INITIAL_USER_NAME );
        }
    }

    @Test
    void shouldFailAlterPasswordUpgradeUser()
    {
        setupRestricted();

        try ( Session session = loginLoopback() )
        {
            assertThatThrownBy( () ->
                    session.run( "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'foo'" ).consume()
            ).hasMessageContaining( "User failed to alter their own password: Command not available with auth disabled." );
        }
    }

    @Test
    void shouldFailStartupWithNoOperatorPassword()
    {
        assertThatThrownBy( () -> getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) ) )
                .hasRootCauseMessage( "No password has been set for the loopback operator. Run `neo4j-admin set-operator-password <password>`." );
    }

    private Session loginLoopback()
    {
        Driver driver = GraphDatabase.driver( LOOPBACK_URI, AuthTokens.basic( "ignored", "bar" ), config );
        driver.verifyConnectivity();
        return driver.session( SessionConfig.builder().withDatabase( SYSTEM_DATABASE_NAME ).build() );
    }

    private Session loginBolt()
    {
        var ip = Inet4Address.getLoopbackAddress().getHostAddress();
        ConnectorPortRegister portRegister =
                ((GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME )).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort bolt = portRegister.getLocalAddress( "bolt" );
        String uri = "neo4j://" + ip + ":" + bolt.getPort();
        Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( INITIAL_USER_NAME, "foo" ), config );
        driver.verifyConnectivity();
        return driver.session( SessionConfig.builder().withDatabase( SYSTEM_DATABASE_NAME ).build() );
    }

    private DatabaseManagementService getEnterpriseManagementService( Map<Setting<?>,Object> config )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDir )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.encryption_level, DISABLED )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "0.0.0.0", 0 ) )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true )
                .setConfig( config )
                .build();
    }

    private void setupRestricted()
    {
        setupOperatorUserAndSystemDatabase( true, true );
    }

    private void setupRestricted( boolean blocked )
    {
        setupOperatorUserAndSystemDatabase( true, blocked );
    }

    private void setupUnrestricted( )
    {
        setupOperatorUserAndSystemDatabase( false, false );
    }

    private void setupOperatorUserAndSystemDatabase( boolean restrictUpgrade, boolean blockDatabase )
    {
        setOperatorPassword();
        final Map<Setting<?>, Object> config =
                Map.of( GraphDatabaseInternalSettings.restrict_upgrade, restrictUpgrade,
                        GraphDatabaseInternalSettings.block_create_drop_database, blockDatabase,
                        GraphDatabaseInternalSettings.block_start_stop_database, blockDatabase );

        enterpriseDbms = getEnterpriseManagementService( config );
        enterpriseDbms.database( SYSTEM_DATABASE_NAME );
    }

    private void setInitialPassword()
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetInitialPasswordCommand( ctx );
        CommandLine.populateCommand( command, "foo" );
        command.execute();
    }

    private void setOperatorPassword()
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetOperatorPasswordCommand( ctx );
        CommandLine.populateCommand( command, "bar" );
        command.execute();
    }
}
