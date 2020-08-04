/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
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
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@ExtendWith( EphemeralFileSystemExtension.class )
class SystemGraphRealmUpgradeIT
{
    private static final String DEFAULT_PASSWORD = "bar";

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

    private void assertLogin( GraphDatabaseAPI database, String username, String password ) throws InvalidAuthTokenException
    {
        AuthManager authManager = database.getDependencyResolver().resolveDependency( AuthManager.class );
        LoginContext loginContext = authManager.login( SecurityTestUtils.authToken( username, password ) );
        assertThat( loginContext.subject().getAuthenticationResult(), oneOf( AuthenticationResult.SUCCESS, AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
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
}
