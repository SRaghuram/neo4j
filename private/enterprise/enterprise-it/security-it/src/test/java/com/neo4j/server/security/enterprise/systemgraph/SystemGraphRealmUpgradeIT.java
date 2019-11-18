/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@TestDirectoryExtension
class SystemGraphRealmUpgradeIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldUpgradeWithDefaultUser()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME );

        // WHEN
        DatabaseManagementService enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            Result users = tx.execute( "SHOW USERS" );
            assertTrue( users.hasNext() );
            Map<String,Object> user = users.next();
            assertThat( user.get( "user" ), equalTo( INITIAL_USER_NAME ) );
            assertThat( (List<String>) user.get( "roles" ), containsInAnyOrder( PredefinedRoles.ADMIN ) );
            assertFalse( users.hasNext() );
            tx.commit();
        }
    }

    @Test
    void shouldUpgradeWithSingleRenamedUser()
    {
        // GIVEN
        createCommunityWithUsers( "Alice" );

        // WHEN
        DatabaseManagementService enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            Result users = tx.execute( "SHOW USERS" );
            assertTrue( users.hasNext() );
            Map<String,Object> user = users.next();
            assertThat( user.get( "user" ), equalTo( "Alice" ) );
            assertThat( (List<String>) user.get( "roles" ), containsInAnyOrder( PredefinedRoles.ADMIN ) );
            assertFalse( users.hasNext() );
            tx.commit();
        }
    }

    @Test
    void shouldUpgradeDefaultUserAndOtherUser()
    {
        // GIVEN
        createCommunityWithUsers( INITIAL_USER_NAME, "Alice" );

        // WHEN
        DatabaseManagementService enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            List<UserResult> users = new ArrayList<>();
            Result result = tx.execute( "SHOW USERS" );
            while ( result.hasNext() )
            {
                Map<String,Object> user = result.next();
                users.add( new UserResult( (String) user.get( "user" ), (List<String>) user.get( "roles" ) ) );
            }
            assertThat( users, containsInAnyOrder(
                    new UserResult( INITIAL_USER_NAME, List.of( PredefinedRoles.ADMIN ) ),
                    new UserResult( "Alice", Collections.emptyList() ) ) );
            tx.commit();
        }
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
        DatabaseManagementService enterpriseDbms = getEnterpriseManagementService();

        // THEN
        GraphDatabaseService database = enterpriseDbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            List<UserResult> users = new ArrayList<>();
            Result result = tx.execute( "SHOW USERS" );
            while ( result.hasNext() )
            {
                Map<String,Object> user = result.next();
                users.add( new UserResult( (String) user.get( "user" ), (List<String>) user.get( "roles" ) ) );
            }
            assertThat( users, containsInAnyOrder(
                    new UserResult( "Alice", List.of( PredefinedRoles.ADMIN ) ),
                    new UserResult( "Bob", Collections.emptyList() ) ) );
            tx.commit();
        }
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
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( testDirectory.getFileSystem() ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
    }

    private void createCommunityWithUsers( String... users )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( testDirectory.getFileSystem() ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, true ).build();

        GraphDatabaseService database = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            tx.execute( String.format( "DROP USER %s", INITIAL_USER_NAME ) ).close();
            for ( String user : users )
            {
                tx.execute( String.format( "CREATE USER %s SET PASSWORD 'bar' CHANGE NOT REQUIRED", user ) ).close();
            }
            tx.commit();
        }
        managementService.shutdown();
    }

    private void setDefaultAdmin( String username )
    {
        final var command = new SetDefaultAdminCommand(
                new ExecutionContext( testDirectory.homeDir().toPath(),
                                      new File( testDirectory.homeDir(), "conf" ).toPath(),
                                      mock( PrintStream.class ),
                                      mock( PrintStream.class ),
                                      testDirectory.getFileSystem() ) );
        CommandLine.populateCommand( command, username );
        command.execute();
    }
}
