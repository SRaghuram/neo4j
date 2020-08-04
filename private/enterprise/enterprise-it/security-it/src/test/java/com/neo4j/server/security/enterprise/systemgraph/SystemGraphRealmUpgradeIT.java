/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.SecurityTestUtils;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@ExtendWith( EphemeralFileSystemExtension.class )
class SystemGraphRealmUpgradeIT
{
    private static final String DEFAULT_PASSWORD = "bar";
    private static final String UPGRADE_USERNAME = Config.defaults().get( GraphDatabaseInternalSettings.upgrade_username );

    @SuppressWarnings( "unused" )
    @Inject
    private FileSystemAbstraction fileSystem;
    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;
    private DatabaseManagementService enterpriseDbms;

    @BeforeEach
    void setup()
    {
        Path graphDir = Path.of( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
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
    void shouldCreateDefaultRoles()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME );

        // WHEN
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            ResourceIterator<Node> roles = tx.findNodes( label( "Role" ) );
            List<String> roleNames = roles.stream().map( role -> role.getProperty( "name" ).toString() ).collect( Collectors.toList() );
            assertThat( roleNames, containsInAnyOrder( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER, PUBLIC ) );
            tx.commit();
        }
        enterpriseDbms.shutdown();
    }

    @Test
    void shouldUpgradeWithDefaultUser()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME );

        // WHEN
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder( new UserResult( INITIAL_USER_NAME, List.of( ADMIN, PUBLIC ) ) ) );
    }

    @Test
    void shouldUpgradeWithSingleCustomUser()
    {
        // GIVEN
        createCommunityWithUsers( "Alice" );

        // WHEN
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder( new UserResult( "Alice", List.of( ADMIN, PUBLIC ) ) ) );
    }

    @Test
    void shouldUpgradeDefaultAndOtherUser()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME, "Alice" );

        // WHEN
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder(
                new UserResult( INITIAL_USER_NAME, List.of( ADMIN, PUBLIC ) ),
                new UserResult( "Alice", List.of( PUBLIC ) ) )
        );
    }

    @Test
    void shouldChangeInitialPasswordAndSetAdmin() throws InvalidAuthTokenException
    {
        setInitialPassword( "abc123" );
        createCommunityWithUsers( INITIAL_USER_NAME );

        // WHEN
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder( new UserResult( INITIAL_USER_NAME, List.of( ADMIN, PUBLIC ) ) ) );
        assertLogin( (GraphDatabaseAPI) database, INITIAL_USER_NAME, "abc123" );
    }

    @Test
    void shouldFailUpgradeWithMultipleUsers()
    {
        // GIVEN
        createCommunityWithUsers( "Alice", "Bob" );

        // WHEN
        Throwable exception = assertThrows( RuntimeException.class, this::getEnterpriseManagementService );
        assertThat( exception.getCause().getMessage(), containsString(
                "No roles defined, and cannot determine which user should be admin. Please use `neo4j-admin set-default-admin` to select an admin." ) );
    }

    @Test
    void shouldFailUpgradeWithNonExistentDefaultAdmin()
    {
        createCommunityWithUsers( "Alice", "Bob" );

        setDefaultAdmin( "Charlie" );

        // WHEN
        Throwable exception = assertThrows( RuntimeException.class, this::getEnterpriseManagementService );
        assertThat( exception.getCause().getMessage(), containsString(
                "No roles defined, and default admin user '" + "Charlie" + "' does not exist. " + "Please use " +
                "`neo4j-admin set-default-admin` to select a valid admin." ) );
    }

    @Test
    void shouldUpgradeUsersWithSetDefaultAdminCommand()
    {
        // GIVEN
        createCommunityWithUsers( "Alice", "Bob" );

        setDefaultAdmin( "Alice" );

        // WHEN
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder(
                new UserResult( "Alice", List.of( ADMIN, PUBLIC ) ),
                new UserResult( "Bob", List.of( PUBLIC ) ) )
        );
    }

    @Test
    void shouldSetCustomAsAdminWhenSetAsDefaultAdmin()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME, "Alice" );

        // WHEN
        setDefaultAdmin( "Alice" );
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder(
                new UserResult( "Alice", List.of( ADMIN, PUBLIC ) ),
                new UserResult( INITIAL_USER_NAME, List.of( PUBLIC ) ) )
        );
    }

    @Test
    void shouldSetInitialUserAsAdminWhenSetAsDefaultAdmin()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME, "Alice" );

        // WHEN
        setDefaultAdmin( INITIAL_USER_NAME );
       enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder(
                new UserResult( INITIAL_USER_NAME, List.of( ADMIN, PUBLIC ) ),
                new UserResult( "Alice", List.of( PUBLIC ) ) )
        );
    }

    @Test
    void shouldSetCustomAsAdminAndInitialPasswordForInitialUser() throws InvalidAuthTokenException
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME, "Alice" );

        // WHEN
        setDefaultAdmin( "Alice" );
        setInitialPassword( "foo" );
        enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        List<UserResult> users = getUsers( database );
        assertThat( users, containsInAnyOrder(
                new UserResult( "Alice", List.of( ADMIN, PUBLIC ) ),
                new UserResult( INITIAL_USER_NAME, List.of( PUBLIC ) ) )
        );
        assertLogin( database, "Alice", DEFAULT_PASSWORD );
        assertLogin( database, INITIAL_USER_NAME, INITIAL_PASSWORD );
    }

    @Test
    void shouldCreateOperatorUserWhenRestrictUpgradeIsEnabled() throws InvalidAuthTokenException
    {
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );

        LoginContext loginContext = assertLoginResult( database, UPGRADE_USERNAME, "bar", AuthenticationResult.SUCCESS );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "CALL dbms.showCurrentUser()" );
            Map<String,Object> row = result.next();

            assertThat( row.get( "username" ), equalTo( UPGRADE_USERNAME ) );
            assertThat( row.get( "roles" ), equalTo( Collections.EMPTY_LIST ) );
            assertThat( row.get( "flags" ), equalTo( Collections.EMPTY_LIST ) );
        }
    }

    @Test
    void shouldNotCreateOperatorUserWhenRestrictUpgradeIsDisabled() throws InvalidAuthTokenException
    {
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, false ) );

        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        assertLoginResult( database, UPGRADE_USERNAME, "bar", AuthenticationResult.FAILURE );
    }

    @Test
    void shouldNotReserveUpgradeUsernameWhenRestrictUpgradeIsDisabled() throws InvalidAuthTokenException
    {
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, false ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );

        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) );
            tx.commit();
        }

        LoginContext loginContext = assertLoginResult( database, UPGRADE_USERNAME, "bar", AuthenticationResult.SUCCESS );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "CALL dbms.showCurrentUser()" );
            Map<String,Object> row = result.next();

            assertThat( row.get( "username" ), equalTo( UPGRADE_USERNAME ) );
            assertThat( row.get( "roles" ), equalTo( List.of( PUBLIC ) ) );
            assertThat( row.get( "flags" ), equalTo( Collections.EMPTY_LIST ) );
        }
    }

    @Test
    void shouldReserveUpgradeUsernameWhenRestrictUpgradeIsEnabled()
    {
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );

        try ( Transaction tx = database.beginTx() )
        {
            assertThrows( QueryExecutionException.class,
                    () -> tx.execute( String.format( "CREATE USER %s SET PASSWORD 'foo' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) ) );
        }
    }

    @Test
    void shouldNotStartDatabaseIfUpgradeUsernameIsTakenByAnotherUser()
    {
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, false ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );

        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) );
            tx.commit();
        }

        enterpriseDbms.shutdown();

        setOperatorPassword( "foo" );

        Throwable exception =
                assertThrows( RuntimeException.class, () -> getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) ) );
        assertThat( exception.getCause().getMessage(),
                containsString( String.format( "The user specified by unsupported.dbms.upgrade_procedure_username (%s) already exists in the system graph. " +
                                "Change the username or delete the user before restricting upgrade.", UPGRADE_USERNAME ) ) );
    }

    @Test
    void shouldNotAllowTransactionToDefaultDatabaseAsUpgradeUser() throws InvalidAuthTokenException
    {
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( DEFAULT_DATABASE_NAME );

        LoginContext loginContext = assertLoginResult( database, UPGRADE_USERNAME, "bar", AuthenticationResult.SUCCESS );

        assertThrows( AuthorizationViolationException.class, () -> database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"dbms.upgrade", "dbms.upgradeStatus", "dbms.upgradeDetails", "dbms.upgradeStatusDetails"} )
    void shouldOnlyAllowOperatorUserToCallUpgradeProcedure( String procedureName ) throws InvalidAuthTokenException
    {
        String query = String.format( "CALL %s()", procedureName );
        setInitialPassword( "foo" );
        setOperatorPassword( "bar" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, true ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );

        LoginContext loginContext = assertLoginResult( database, UPGRADE_USERNAME, "bar", AuthenticationResult.SUCCESS );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( query );
            assertTrue( result.hasNext() );
        }

        LoginContext adminUser = assertLoginResult( database, INITIAL_USER_NAME, "foo", AuthenticationResult.SUCCESS );
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, adminUser ) )
        {
            Throwable exception = assertThrows( QueryExecutionException.class, () -> tx.execute( query ) );
            assertThat( exception.getMessage(), containsString( "Execution of this procedure has been restricted by the system." ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"dbms.upgrade", "dbms.upgradeStatus", "dbms.upgradeDetails", "dbms.upgradeStatusDetails"} )
    void shouldNotAllowOperatorUserToCallUpgradeProcedure( String procedureName ) throws InvalidAuthTokenException
    {
        String query = String.format( "CALL %s()", procedureName );
        setInitialPassword( "foo" );
        enterpriseDbms = getEnterpriseManagementService( Map.of( GraphDatabaseInternalSettings.restrict_upgrade, false ) );
        GraphDatabaseAPI database = (GraphDatabaseAPI) enterpriseDbms.database( SYSTEM_DATABASE_NAME );

        // create user with same name as upgrade_username setting
        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", UPGRADE_USERNAME ) );
            tx.commit();
        }

        LoginContext loginContext = assertLoginResult( database, UPGRADE_USERNAME, "bar", AuthenticationResult.SUCCESS );

        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Throwable exception = assertThrows( QueryExecutionException.class, () -> tx.execute( query ) );
            assertThat( exception.getMessage(), containsString( AuthorizationViolationException.PERMISSION_DENIED ) );
        }

        LoginContext adminUser = assertLoginResult( database, INITIAL_USER_NAME, "foo", AuthenticationResult.SUCCESS );
        try ( Transaction tx = database.beginTransaction( KernelTransaction.Type.EXPLICIT, adminUser ) )
        {
            Result result = tx.execute( query );
            assertTrue( result.hasNext() );
        }
    }

    private void assertLogin( GraphDatabaseAPI database, String username, String password ) throws InvalidAuthTokenException
    {
        assertLoginResult( database, username, password, AuthenticationResult.SUCCESS, AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    private LoginContext assertLoginResult( GraphDatabaseAPI database, String username, String password, AuthenticationResult... authResults )
            throws InvalidAuthTokenException
    {
        AuthManager authManager = database.getDependencyResolver().resolveDependency( AuthManager.class );
        LoginContext loginContext = authManager.login( SecurityTestUtils.authToken( username, password ) );
        assertThat( loginContext.subject().getAuthenticationResult(), oneOf( authResults ) );

        return loginContext;
    }

    private List<UserResult> getUsers( GraphDatabaseService database )
    {
        List<UserResult> users = new ArrayList<>();
        try ( Transaction tx = database.beginTx() )
        {
            Result result = tx.execute( "SHOW USERS" );
            while ( result.hasNext() )
            {
                Map<String,Object> user = result.next();
                //noinspection unchecked
                users.add( new UserResult( (String) user.get( "user" ), (List<String>) user.get( "roles" ) ) );
            }
            tx.commit();
        }
        return users;
    }

    private static class UserResult
    {
        private final String user;
        private final List<String> roles;

        UserResult( String user, List<String> roles )
        {
            this.user = user;
            this.roles = roles;
        }

        @Override
        public int hashCode()
        {
            return roles.hashCode() + 31 * user.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( !(obj instanceof UserResult ) )
            {
                return false;
            }
            UserResult other = (UserResult) obj;
            return other.user.equals( user ) && other.roles.equals( roles );
        }
    }

    private DatabaseManagementService getEnterpriseManagementService()
    {
        return getEnterpriseManagementService( Collections.emptyMap() );
    }

    private DatabaseManagementService getEnterpriseManagementService( Map<Setting<?>,Object> config )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDir )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true )
                .setConfig( config )
                .build();
    }

    private DatabaseManagementService getCommunityManagementService()
    {
        return new TestDatabaseManagementServiceBuilder( homeDir )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
    }

    private void createCommunityWithUsers( String... users )
    {
        DatabaseManagementService managementService = getCommunityManagementService();

        GraphDatabaseService database = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            boolean dropInitialUser = true;
            for ( String user : users )
            {
                if ( user.equals( INITIAL_USER_NAME ) )
                {
                    dropInitialUser = false;
                }
                else
                {
                    tx.execute( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", user, DEFAULT_PASSWORD ) ).close();
                }
            }
            if ( dropInitialUser )
            {
                tx.execute( String.format( "DROP USER %s", INITIAL_USER_NAME ) ).close();
            }
            tx.commit();
        }
        managementService.shutdown();
    }

    private void setDefaultAdmin( String username )
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetDefaultAdminCommand( ctx );
        CommandLine.populateCommand( command, username );
        command.execute();
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
