/*
 * Copyright (c) "Neo4j"
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
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.configuration.Config;
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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.SecurityTestUtils;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@ExtendWith( EphemeralFileSystemExtension.class )
class OperatorUserIT
{
    private static final String UPGRADE_USERNAME = Config.defaults().get( GraphDatabaseInternalSettings.upgrade_username );

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
    void shouldCreateOperatorUserWhenRestrictUpgradeIsEnabled() throws InvalidAuthTokenException
    {
        GraphDatabaseAPI database = setupRestricted();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );
        SecurityContext securityContext = loginContext.authorize( LoginContext.IdLookup.EMPTY, SYSTEM_DATABASE_NAME );
        assertThat( securityContext.roles() ).isEmpty();

        assertThatThrownBy( () -> loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) )
                .isInstanceOf( AuthorizationViolationException.class )
                .hasMessage( "Database access is not allowed for user 'upgrade_user' with roles []." );
    }

    @Test
    void shouldNotCreateOperatorUserWhenRestrictUpgradeIsDisabled() throws InvalidAuthTokenException
    {
        GraphDatabaseAPI database = setupUnrestricted();
        assertLoginFailure( database, UPGRADE_USERNAME, "bar" );
    }

    @Test
    void shouldNotReserveUpgradeUsernameWhenRestrictUpgradeIsDisabled() throws InvalidAuthTokenException
    {
        GraphDatabaseAPI database = setupUnrestricted();

        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) );
            tx.commit();
        }

        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "CALL dbms.showCurrentUser()" );
            Map<String,Object> row = result.next();

            assertThat( row.get( "username" ) ).isEqualTo( UPGRADE_USERNAME );
            assertThat( row.get( "roles" ) ).isEqualTo( List.of( PUBLIC ) );
            assertThat( row.get( "flags" ) ).isEqualTo( Collections.emptyList() );
        }
    }

    @Test
    void shouldReserveUpgradeUsernameWhenRestrictUpgradeIsEnabled()
    {
        GraphDatabaseAPI database = setupRestricted();

        try ( Transaction tx = database.beginTx() )
        {
            assertThatThrownBy( () -> tx.execute( String.format( "CREATE USER %s SET PASSWORD 'foo' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) ) )
                    .hasMessageContaining( String.format( "Username '%s' is reserved by the system.", UPGRADE_USERNAME ) );
        }
    }

    @Test
    void shouldNotStartDatabaseIfUpgradeUsernameIsTakenByAnotherUser()
    {
        GraphDatabaseAPI database = setupUnrestricted();

        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) );
            tx.commit();
        }

        enterpriseDbms.shutdown();

        setOperatorPassword( "foo" );

        assertThatThrownBy( () -> getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) ) )
                .hasRootCauseMessage( String.format(
                        "The user specified by unsupported.dbms.upgrade_procedure_username (%s) already exists in the system graph. " +
                        "Change the username or delete the user before restricting upgrade.", UPGRADE_USERNAME ) );
    }

    @Test
    void shouldNotAllowTransactionToDefaultDatabaseAsUpgradeUser() throws InvalidAuthTokenException
    {
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME );

        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        assertThatThrownBy( () -> database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
                .isInstanceOf( AuthorizationViolationException.class );
    }

    @Test
    void shouldBeAbleToGetRoutingTableAsOperator()
    {
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) );
        ConnectorPortRegister portRegister =
                ((GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME )).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort bolt = portRegister.getLocalAddress( "bolt" );
        var uri = "neo4j://" + bolt.toString();

        try ( Driver driver = GraphDatabase.driver( uri, AuthTokens.basic( UPGRADE_USERNAME, "bar" ), config );
              Session session = driver.session( SessionConfig.builder().withDatabase( SYSTEM_DATABASE_NAME ).build() ) )
        {
            session.run( "CALL dbms.upgrade()" ).consume();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"dbms.upgrade", "dbms.upgradeStatus", "dbms.upgradeDetails", "dbms.upgradeStatusDetails"} )
    void shouldOnlyAllowOperatorUserToCallUpgradeProcedure( String procedureName ) throws InvalidAuthTokenException
    {
        String query = String.format( "CALL %s()", procedureName );
        setInitialPassword( "foo" );
        GraphDatabaseAPI database = setupRestricted();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( query );
            assertThat( result.hasNext() ).isTrue();
        }

        LoginContext adminUser = assertLoginSuccess( database, INITIAL_USER_NAME, "foo" );
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, adminUser ) )
        {
            assertThatThrownBy( () -> tx.execute( query ) ).hasMessageContaining( "Execution of this procedure has been restricted by the system." );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"dbms.upgrade", "dbms.upgradeStatus", "dbms.upgradeDetails", "dbms.upgradeStatusDetails"} )
    void shouldNotAllowUserWithOperatorNameToCallUpgradeProcedure( String procedureName ) throws InvalidAuthTokenException
    {
        String query = String.format( "CALL %s()", procedureName );
        setInitialPassword( "foo" );
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( query ) ).hasMessageContaining( "Executing admin procedure is not allowed" );
        }

        LoginContext adminUser = assertLoginSuccess( database, INITIAL_USER_NAME, "foo" );
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, adminUser ) )
        {
            Result result = tx.execute( query );
            assertThat( result.hasNext() ).isTrue();
        }
    }

    @Test
    void shouldAllowOperatorUserToShowDatabases() throws InvalidAuthTokenException
    {
        GraphDatabaseAPI database = setupRestricted();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASES" );
            final Set<Object> names = result.columnAs( "name" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME ), names );
        }

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASE system" );
            assertThat( result.hasNext() ).isTrue();
        }

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DEFAULT DATABASE" );
            assertThat( result.hasNext() ).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToCreateDatabase( boolean blocked ) throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupRestricted( blocked );
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            tx.execute( "CREATE DATABASE operational" );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASES" );
            final Set<Object> names = result.columnAs( "name" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, "operational" ), names );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToCreateDatabase() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( "CREATE DATABASE operational" ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "CREATE DATABASE is not supported" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToCreateDatabaseIfNotExists( boolean blocked ) throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupRestricted( blocked );
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Existing database
            tx.execute( String.format( "CREATE DATABASE %s IF NOT EXISTS", DEFAULT_DATABASE_NAME ) );
            // Non-existing database
            tx.execute( "CREATE DATABASE operational IF NOT EXISTS" );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASES" );
            final Set<Object> names = result.columnAs( "name" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, "operational" ), names );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToCreateDatabaseIfNotExists() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Existing database
            assertThatThrownBy( () -> tx.execute( String.format( "CREATE DATABASE %s IF NOT EXISTS", DEFAULT_DATABASE_NAME ) ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "CREATE DATABASE is not supported" );
        }

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Non-existing database
            assertThatThrownBy( () -> tx.execute( "CREATE DATABASE operational IF NOT EXISTS" ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "CREATE DATABASE is not supported" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToCreateOrReplaceDatabase( boolean blocked ) throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupRestricted( blocked );
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Existing database
            tx.execute( String.format( "CREATE OR REPLACE DATABASE %s", DEFAULT_DATABASE_NAME ) );
            // Non-existing database
            tx.execute( "CREATE OR REPLACE DATABASE operational" );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASES" );
            final Set<Object> names = result.columnAs( "name" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, "operational" ), names );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToCreateOrReplaceDatabase() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Existing database
            assertThatThrownBy( () -> tx.execute( String.format( "CREATE OR REPLACE DATABASE %s", DEFAULT_DATABASE_NAME ) ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "CREATE DATABASE is not supported" );
        }

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Non-existing database
            assertThatThrownBy( () -> tx.execute( "CREATE OR REPLACE DATABASE operational" ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "CREATE DATABASE is not supported" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToDropDatabase( boolean blocked ) throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupRestricted( blocked );
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            tx.execute( String.format( "DROP DATABASE %s", DEFAULT_DATABASE_NAME ) );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASES" );
            final Set<Object> names = result.columnAs( "name" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( SYSTEM_DATABASE_NAME ), names );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToDropDatabase() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( String.format( "DROP DATABASE %s", DEFAULT_DATABASE_NAME ) ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "DROP DATABASE is not supported" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToDropDatabaseIfExists( boolean blocked ) throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupRestricted( blocked );
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Existing database
            tx.execute( String.format( "DROP DATABASE %s IF EXISTS", DEFAULT_DATABASE_NAME ) );
            // Non-existing database
            tx.execute( "DROP DATABASE operational IF EXISTS" );
            tx.commit();
        }

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DATABASES" );
            final Set<Object> names = result.columnAs( "name" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( SYSTEM_DATABASE_NAME ), names );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToDropDatabaseIfExists() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Existing database
            assertThatThrownBy( () -> tx.execute( String.format( "DROP DATABASE %s IF EXISTS", DEFAULT_DATABASE_NAME ) ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "DROP DATABASE is not supported" );
        }

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Non-existing database
            assertThatThrownBy( () -> tx.execute( "DROP DATABASE operational IF EXISTS" ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "DROP DATABASE is not supported" );
        }
    }

    @ParameterizedTest
    @ValueSource( booleans = { true, false } )
    void shouldAllowOperatorUserToStartAndStopDatabase( boolean blocked ) throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupRestricted( blocked );
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Stop online database
            tx.execute( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DEFAULT DATABASE" );
            final Set<Object> status = result.columnAs( "currentStatus" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( "offline" ), status );
        }

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Stop offline database
            tx.execute( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DEFAULT DATABASE" );
            final Set<Object> status = result.columnAs( "currentStatus" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( "offline" ), status );
        }

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Start offline database
            tx.execute( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DEFAULT DATABASE" );
            final Set<Object> status = result.columnAs( "currentStatus" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( "online" ), status );
        }

        // WHEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            // Start online database
            tx.execute( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) );
            tx.commit();
        }

        // THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW DEFAULT DATABASE" );
            final Set<Object> status = result.columnAs( "currentStatus" ).stream().collect( Collectors.toSet() );
            assertEquals( Set.of( "online" ), status );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToStartDatabase() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( String.format( "START DATABASE %s", DEFAULT_DATABASE_NAME ) ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "START DATABASE is not supported" );
        }
    }

    @Test
    void shouldNotAllowUserWithOperatorNameToStopDatabase() throws InvalidAuthTokenException
    {
        // GIVEN
        GraphDatabaseAPI database = setupUserWithOperatorNameAndSystemDatabase();
        LoginContext loginContext = assertLoginSuccess( database, UPGRADE_USERNAME, "bar" );

        // WHEN .. THEN
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( String.format( "STOP DATABASE %s", DEFAULT_DATABASE_NAME ) ) )
                    .isInstanceOf( UnsupportedOperationException.class )
                    .hasMessageContaining( "STOP DATABASE is not supported" );
        }
    }

    @Test
    void shouldNotListUpgradeUser() throws InvalidAuthTokenException
    {
        setInitialPassword( "baz" );
        GraphDatabaseAPI database = setupRestricted();

        LoginContext loginContext = login( database, INITIAL_USER_NAME, "baz" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "SHOW USERS YIELD user WHERE user = $username", Map.of( "username", UPGRADE_USERNAME ) );
            assertThat( result.hasNext() ).isFalse();
        }
    }

    @Test
    void shouldFailGrantRoleToUpgradeUser() throws InvalidAuthTokenException
    {
        setInitialPassword( "baz" );
        GraphDatabaseAPI database = setupRestricted();

        LoginContext loginContext = login( database, INITIAL_USER_NAME, "baz" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( "GRANT ROLE admin TO $username", Map.of( "username", UPGRADE_USERNAME ) ) )
                    .hasMessageContaining( String.format( "Failed to grant role 'admin' to user '%s': User does not exist.", UPGRADE_USERNAME ) );
        }
    }

    @Test
    void shouldFailAlterPasswordUpgradeUser() throws InvalidAuthTokenException
    {
        GraphDatabaseAPI database = setupRestricted();
        LoginContext loginContext = login( database, UPGRADE_USERNAME, "bar" );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            assertThatThrownBy( () -> tx.execute( "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'foo'" ) )
                    .hasMessageContaining( String.format( "User '%s' failed to alter their own password: User does not exist.", UPGRADE_USERNAME ) );
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    private void assertLoginFailure( GraphDatabaseAPI database, String username, String password )
            throws InvalidAuthTokenException
    {
        LoginContext loginContext = login( database, username, password );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.FAILURE );
    }

    private LoginContext assertLoginSuccess( GraphDatabaseAPI database, String username, String password )
            throws InvalidAuthTokenException
    {
        LoginContext loginContext = login( database, username, password );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.SUCCESS );
        return loginContext;
    }

    private LoginContext login( GraphDatabaseAPI database, String username, String password ) throws InvalidAuthTokenException
    {
        AuthManager authManager = database.getDependencyResolver().resolveDependency( AuthManager.class );
        return authManager.login( SecurityTestUtils.authToken( username, password ) );
    }

    private DatabaseManagementService getEnterpriseManagementService( Map<Setting<?>,Object> config )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDir )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.encryption_level, DISABLED )
                .setConfig( BoltConnector.listen_address, new SocketAddress(  InetAddress.getLoopbackAddress().getHostAddress(), 0 ) )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true )
                .setConfig( config )
                .build();
    }

    private GraphDatabaseAPI setupRestricted()
    {
        return setupOperatorUserAndSystemDatabase( true, true );
    }

    private GraphDatabaseAPI setupRestricted( boolean blocked )
    {
        return setupOperatorUserAndSystemDatabase( true, blocked );
    }

    private GraphDatabaseAPI setupUnrestricted( )
    {
        return setupOperatorUserAndSystemDatabase( false, false );
    }

    private GraphDatabaseAPI setupUserWithOperatorNameAndSystemDatabase()
    {
        GraphDatabaseAPI database = setupOperatorUserAndSystemDatabase( false, true );

        // create user with same name as upgrade_username setting and grant it all database related privileges
        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) );
            tx.execute( "CREATE role databaseOperator" );
            tx.execute( "GRANT DATABASE MANAGEMENT ON DBMS TO databaseOperator" );
            tx.execute( "GRANT START ON DATABASE * TO databaseOperator" );
            tx.execute( "GRANT STOP ON DATABASE * TO databaseOperator" );
            tx.execute( String.format( "GRANT ROLE databaseOperator TO %s", UPGRADE_USERNAME ) );
            tx.commit();
        }
        return database;
    }

    private GraphDatabaseAPI setupOperatorUserAndSystemDatabase( boolean restrictUpgrade, boolean blockDatabase )
    {
        setOperatorPassword( "bar" );
        final Map<Setting<?>, Object> config =
                Map.of( GraphDatabaseInternalSettings.restrict_upgrade, restrictUpgrade,
                        GraphDatabaseInternalSettings.block_create_drop_database, blockDatabase,
                        GraphDatabaseInternalSettings.block_start_stop_database, blockDatabase );

        enterpriseDbms = getEnterpriseManagementService( config );
        return (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );
    }

    private void setInitialPassword( String password )
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetInitialPasswordCommand( ctx );
        CommandLine.populateCommand( command, password );
        command.execute();
    }

    private void setOperatorPassword( String password )
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetOperatorPasswordCommand( ctx );
        CommandLine.populateCommand( command, password );
        command.execute();
    }
}
