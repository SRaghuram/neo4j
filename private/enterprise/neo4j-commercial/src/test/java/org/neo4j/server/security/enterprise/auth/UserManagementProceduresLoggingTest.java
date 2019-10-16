/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.test.assertion.Assert.assertException;

public class UserManagementProceduresLoggingTest
{
    protected TestUserManagementProcedures userManagementProcedures;
    private RoleManagementProcedures roleManagementProcedures;
    private AssertableLogProvider log;
    private EnterpriseSecurityContext matsContext;
    private EnterpriseUserManager generalUserManager;

    @Before
    public void setUp() throws Throwable
    {
        log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        userManagementProcedures = new TestUserManagementProcedures();
        userManagementProcedures.graph = mock( GraphDatabaseAPI.class );
        userManagementProcedures.securityLog = securityLog;

        roleManagementProcedures = new RoleManagementProcedures();
        roleManagementProcedures.graph = userManagementProcedures.graph;
        roleManagementProcedures.securityLog = userManagementProcedures.securityLog;

        generalUserManager = getUserManager();
        EnterpriseSecurityContext adminContext =
                new EnterpriseSecurityContext( new MockAuthSubject( "admin" ), AccessMode.Static.FULL, Collections.emptySet(), true );
        matsContext =
                new EnterpriseSecurityContext( new MockAuthSubject( "mats" ), AccessMode.Static.NONE, Collections.emptySet(), false );

        setSubject( adminContext );
        log.clear();
    }

    private void setSubject( EnterpriseSecurityContext securityContext )
    {
        PersonalUserManager userManager = new PersonalUserManager( generalUserManager, securityContext.subject(),
                                                                   userManagementProcedures.securityLog, securityContext.isAdmin() );
        userManagementProcedures.securityContext = securityContext;
        userManagementProcedures.userManager = userManager;
        roleManagementProcedures.securityContext = securityContext;
        roleManagementProcedures.userManager = userManager;
    }

    protected EnterpriseUserManager getUserManager() throws Throwable
    {
        InternalFlatFileRealm realm = new InternalFlatFileRealm(
                                            new InMemoryUserRepository(),
                                            new InMemoryRoleRepository(),
                                            new BasicPasswordPolicy(),
                                            mock( AuthenticationStrategy.class ),
                                            mock( JobScheduler.class ),
                                            new InMemoryUserRepository(),
                                            new InMemoryUserRepository()
                                        );
        realm.start(); // creates default user and roles
        return realm;
    }

    @Test
    public void shouldLogCreatingUser() throws Throwable
    {
        userManagementProcedures.createUser( "andres", "el password", true );
        userManagementProcedures.createUser( "mats", "el password", false );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "andres", ", with password change required" ),
                info( "[admin]: created user `%s`%s", "mats", "" )
            );
    }

    @Test
    public void shouldLogFailureToCreateUser()
    {
        catchInvalidArguments( () -> userManagementProcedures.createUser( null, "pw", true ) );
        catchInvalidArguments( () -> userManagementProcedures.createUser( "", "pw", true ) );
        catchInvalidArguments( () -> userManagementProcedures.createUser( "andres", "", true ) );
        catchInvalidArguments( () -> userManagementProcedures.createUser( "mats", null, true ) );
        catchInvalidArguments( () -> userManagementProcedures.createUser( "neo4j", "nonEmpty", true ) );

        log.assertExactly(
                error( "[admin]: tried to create user `%s`: %s", null, "The provided username is empty." ),
                error( "[admin]: tried to create user `%s`: %s", "", "The provided username is empty." ),
                error( "[admin]: tried to create user `%s`: %s", "andres", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "mats", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "neo4j", "The specified user 'neo4j' already exists." )
        );
    }

    @Test
    public void shouldLogUnauthorizedCreatingUser()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> userManagementProcedures.createUser( "andres", "", true ) );

        log.assertExactly( error( "[mats]: tried to create user `%s`: %s", "andres", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogDeletingUser() throws Throwable
    {
        userManagementProcedures.createUser( "andres", "el password", false );
        userManagementProcedures.deleteUser( "andres" );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "andres", "" ),
                info( "[admin]: deleted user `%s`", "andres" ) );
    }

    @Test
    public void shouldLogDeletingNonExistentUser()
    {
        catchInvalidArguments( () -> userManagementProcedures.deleteUser( "andres" ) );

        log.assertExactly( error( "[admin]: tried to delete user `%s`: %s", "andres", "User 'andres' does not exist." ) );
    }

    @Test
    public void shouldLogUnauthorizedDeleteUser()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> userManagementProcedures.deleteUser( ADMIN ) );

        log.assertExactly( error( "[mats]: tried to delete user `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogAddingRoleToUser() throws Throwable
    {
        userManagementProcedures.createUser( "mats", "neo4j", false );
        roleManagementProcedures.addRoleToUser( ARCHITECT, "mats" );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "mats", "" ),
                info( "[admin]: added role `%s` to user `%s`", ARCHITECT, "mats" ) );
    }

    @Test
    public void shouldLogFailureToAddRoleToUser() throws Throwable
    {
        userManagementProcedures.createUser( "mats", "neo4j", false );
        catchInvalidArguments( () -> roleManagementProcedures.addRoleToUser( "null", "mats" ) );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "mats", "" ),
                error( "[admin]: tried to add role `%s` to user `%s`: %s", "null", "mats", "Role 'null' does not exist." ) );
    }

    @Test
    public void shouldLogUnauthorizedAddingRole()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> roleManagementProcedures.addRoleToUser( ADMIN, "mats" ) );

        log.assertExactly( error( "[mats]: tried to add role `%s` to user `%s`: %s", ADMIN, "mats", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogRemovalOfRoleFromUser() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", false );
        roleManagementProcedures.addRoleToUser( READER, "mats" );
        log.clear();

        // When
        roleManagementProcedures.removeRoleFromUser( READER, "mats" );

        // Then
        log.assertExactly( info( "[admin]: removed role `%s` from user `%s`", READER, "mats" ) );
    }

    @Test
    public void shouldLogFailureToRemoveRoleFromUser() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", false );
        roleManagementProcedures.addRoleToUser( READER, "mats" );
        log.clear();

        // When
        catchInvalidArguments( () -> roleManagementProcedures.removeRoleFromUser( "notReader", "mats" ) );
        catchInvalidArguments( () -> roleManagementProcedures.removeRoleFromUser( READER, "notMats" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to remove role `%s` from user `%s`: %s", "notReader", "mats", "Role 'notReader' does not exist." ),
                error( "[admin]: tried to remove role `%s` from user `%s`: %s", READER, "notMats", "User 'notMats' does not exist." )
        );
    }

    @Test
    public void shouldLogUnauthorizedRemovingRole()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> roleManagementProcedures.removeRoleFromUser( ADMIN, ADMIN ) );

        log.assertExactly( error( "[mats]: tried to remove role `%s` from user `%s`: %s", ADMIN, ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogUserPasswordChanges() throws IOException, InvalidArgumentsException
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", true );
        log.clear();

        // When
        userManagementProcedures.changeUserPassword( "mats", "longPassword", false );
        userManagementProcedures.changeUserPassword( "mats", "longerPassword", true );

        setSubject( matsContext );
        userManagementProcedures.changeUserPassword( "mats", "evenLongerPassword", false );

        userManagementProcedures.changePassword( "superLongPassword", false );
        userManagementProcedures.changePassword( "infinitePassword", true );

        // Then
        log.assertExactly(
                info( "[admin]: changed password for user `%s`%s", "mats", "" ),
                info( "[admin]: changed password for user `%s`%s", "mats", ", with password change required" ),
                info( "[mats]: changed password%s", "" ),
                info( "[mats]: changed password%s", "" ),
                info( "[mats]: changed password%s", ", with password change required" )
        );
    }

    @Test
    public void shouldLogFailureToChangeUserPassword() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "andres", "neo4j", true );
        log.clear();

        // When
        catchInvalidArguments( () -> userManagementProcedures.changeUserPassword( "andres", "neo4j", false ) );
        catchInvalidArguments( () -> userManagementProcedures.changeUserPassword( "andres", "", false ) );
        catchInvalidArguments( () -> userManagementProcedures.changeUserPassword( "notAndres", "good password", false ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to change password for user `%s`: %s",
                        "andres", "Old password and new password cannot be the same." ),
                error( "[admin]: tried to change password for user `%s`: %s",
                        "andres", "A password cannot be empty." ),
                error( "[admin]: tried to change password for user `%s`: %s",
                        "notAndres", "User 'notAndres' does not exist." )
        );
    }

    @Test
    public void shouldLogFailureToChangeOwnPassword() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", true );
        setSubject( matsContext );
        log.clear();

        // When
        catchInvalidArguments( () -> userManagementProcedures.changeUserPassword( "mats", "neo4j", false ) );
        catchInvalidArguments( () -> userManagementProcedures.changeUserPassword( "mats", "", false ) );

        catchInvalidArguments( () -> userManagementProcedures.changePassword( null, false ) );
        catchInvalidArguments( () -> userManagementProcedures.changePassword( "", false ) );
        catchInvalidArguments( () -> userManagementProcedures.changePassword( "neo4j", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change password: %s", "Old password and new password cannot be the same." ),
                error( "[mats]: tried to change password: %s", "A password cannot be empty." ),
                error( "[mats]: tried to change password: %s", "A password cannot be empty." ),
                error( "[mats]: tried to change password: %s", "A password cannot be empty." ),
                error( "[mats]: tried to change password: %s", "Old password and new password cannot be the same." )
        );
    }

    @Test
    public void shouldLogUnauthorizedChangePassword() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "andres", "neo4j", true );
        log.clear();
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> userManagementProcedures.changeUserPassword( "andres", "otherPw", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change password for user `%s`: %s", "andres", PERMISSION_DENIED )
        );
    }

    @Test
    public void shouldLogSuspendUser() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", false );
        log.clear();

        // When
        userManagementProcedures.suspendUser( "mats" );
        userManagementProcedures.suspendUser( "mats" );

        // Then
        log.assertExactly(
                info( "[admin]: suspended user `%s`", "mats" ),
                info( "[admin]: suspended user `%s`", "mats" )
        );
    }

    @Test
    public void shouldLogFailureToSuspendUser() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", false );
        log.clear();

        // When
        catchInvalidArguments( () -> userManagementProcedures.suspendUser( "notMats" ) );
        catchInvalidArguments( () -> userManagementProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to suspend user `%s`: %s", "notMats", "User 'notMats' does not exist." ),
                error( "[admin]: tried to suspend user `%s`: %s", "admin", "Suspending yourself (user 'admin') is not allowed." )
        );
    }

    @Test
    public void shouldLogUnauthorizedSuspendUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> userManagementProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to suspend user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    @Test
    public void shouldLogActivateUser() throws Throwable
    {
        // Given
        userManagementProcedures.createUser( "mats", "neo4j", false );
        userManagementProcedures.suspendUser( "mats" );
        log.clear();

        // When
        userManagementProcedures.activateUser( "mats", false );
        userManagementProcedures.activateUser( "mats", false );

        // Then
        log.assertExactly(
                info( "[admin]: activated user `%s`", "mats" ),
                info( "[admin]: activated user `%s`", "mats" )
        );
    }

    @Test
    public void shouldLogFailureToActivateUser()
    {
        // When
        catchInvalidArguments( () -> userManagementProcedures.activateUser( "notMats", false ) );
        catchInvalidArguments( () -> userManagementProcedures.activateUser( ADMIN, false ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to activate user `%s`: %s", "notMats", "User 'notMats' does not exist." ),
                error( "[admin]: tried to activate user `%s`: %s", ADMIN, "Activating yourself (user 'admin') is not allowed." )
        );
    }

    @Test
    public void shouldLogUnauthorizedActivateUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> userManagementProcedures.activateUser( "admin", true ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to activate user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    @Test
    public void shouldLogCreatingRole() throws Throwable
    {
        // When
        roleManagementProcedures.createRole( "role" );

        // Then
        log.assertExactly( info( "[admin]: created role `%s`", "role" ) );
    }

    @Test
    public void shouldLogFailureToCreateRole() throws Throwable
    {
        // Given
        roleManagementProcedures.createRole( "role" );
        log.clear();

        // When
        catchInvalidArguments( () -> roleManagementProcedures.createRole( null ) );
        catchInvalidArguments( () -> roleManagementProcedures.createRole( "" ) );
        catchInvalidArguments( () -> roleManagementProcedures.createRole( "role" ) );
        catchInvalidArguments( () -> roleManagementProcedures.createRole( "!@#$" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to create role `%s`: %s", null, "The provided role name is empty." ),
                error( "[admin]: tried to create role `%s`: %s", "", "The provided role name is empty." ),
                error( "[admin]: tried to create role `%s`: %s", "role", "The specified role 'role' already exists." ),
                error( "[admin]: tried to create role `%s`: %s", "!@#$",
                        "Role name '!@#$' contains illegal characters. Use simple ascii characters and numbers." )
        );
    }

    @Test
    public void shouldLogUnauthorizedCreateRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> roleManagementProcedures.createRole( "role" ) );

        // Then
        log.assertExactly( error("[mats]: tried to create role `%s`: %s", "role", PERMISSION_DENIED) );
    }

    @Test
    public void shouldLogDeletingRole() throws Exception
    {
        // Given
        roleManagementProcedures.createRole( "foo" );
        log.clear();

        // When
        roleManagementProcedures.deleteRole( "foo" );

        // Then
        log.assertExactly( info( "[admin]: deleted role `%s`", "foo" ) );
    }

    @Test
    public void shouldLogFailureToDeleteRole()
    {
        // When
        catchInvalidArguments( () -> roleManagementProcedures.deleteRole( null ) );
        catchInvalidArguments( () -> roleManagementProcedures.deleteRole( "" ) );
        catchInvalidArguments( () -> roleManagementProcedures.deleteRole( "foo" ) );
        catchInvalidArguments( () -> roleManagementProcedures.deleteRole( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to delete role `%s`: %s", null, "Role 'null' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", "", "Role '' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", "foo", "Role 'foo' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", ADMIN, "'admin' is a predefined role and can not be deleted." )
        );
    }

    @Test
    public void shouldLogUnauthorizedDeletingRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> roleManagementProcedures.deleteRole( ADMIN ) );

        // Then
        log.assertExactly( error( "[mats]: tried to delete role `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogIfUnexpectedErrorTerminatingTransactions() throws Exception
    {
        // Given
        userManagementProcedures.createUser( "johan", "neo4j", false );
        userManagementProcedures.failTerminateTransaction();
        log.clear();

        // When
        assertException( () -> userManagementProcedures.deleteUser( "johan" ), RuntimeException.class, "Unexpected error" );

        // Then
        log.assertExactly(
                info( "[admin]: deleted user `%s`", "johan" ),
                error( "[admin]: failed to terminate running transaction and bolt connections for user `%s` following %s: %s",
                        "johan", "deletion", "Unexpected error" )
        );
    }

    @Test
    public void shouldLogUnauthorizedListUsers()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> userManagementProcedures.listUsers() );

        log.assertExactly( error( "[mats]: tried to list users: %s", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogUnauthorizedListRoles()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> roleManagementProcedures.listRoles() );

        log.assertExactly( error( "[mats]: tried to list roles: %s", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogFailureToListRolesForUser()
    {
        // Given

        // When
        catchInvalidArguments( () -> roleManagementProcedures.listRolesForUser( null ) );
        catchInvalidArguments( () -> roleManagementProcedures.listRolesForUser( "" ) );
        catchInvalidArguments( () -> roleManagementProcedures.listRolesForUser( "nonExistent" ) );

        log.assertExactly(
                error( "[admin]: tried to list roles for user `%s`: %s", null, "User 'null' does not exist." ),
                error( "[admin]: tried to list roles for user `%s`: %s", "", "User '' does not exist." ),
                error( "[admin]: tried to list roles for user `%s`: %s", "nonExistent", "User 'nonExistent' does not exist." )
        );
    }

    @Test
    public void shouldLogUnauthorizedListRolesForUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> roleManagementProcedures.listRolesForUser( "user" ) );

        log.assertExactly( error( "[mats]: tried to list roles for user `%s`: %s", "user", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogFailureToListUsersForRole()
    {
        // Given

        // When
        catchInvalidArguments( () -> roleManagementProcedures.listUsersForRole( null ) );
        catchInvalidArguments( () -> roleManagementProcedures.listUsersForRole( "" ) );
        catchInvalidArguments( () -> roleManagementProcedures.listUsersForRole( "nonExistent" ) );

        log.assertExactly(
                error( "[admin]: tried to list users for role `%s`: %s", null, "Role 'null' does not exist." ),
                error( "[admin]: tried to list users for role `%s`: %s", "", "Role '' does not exist." ),
                error( "[admin]: tried to list users for role `%s`: %s", "nonExistent", "Role 'nonExistent' does not exist." )
        );
    }

    @Test
    public void shouldLogUnauthorizedListUsersForRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> roleManagementProcedures.listUsersForRole( "role" ) );

        log.assertExactly( error( "[mats]: tried to list users for role `%s`: %s", "role", PERMISSION_DENIED ) );
    }

    private void catchInvalidArguments( ThrowingAction<Exception> f )
    {
        assertException( f, InvalidArgumentsException.class );
    }

    private void catchAuthorizationViolation( ThrowingAction<Exception> f )
    {
        assertException( f, AuthorizationViolationException.class );
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, (Object[]) arguments );
    }

    private static class MockAuthSubject implements AuthSubject
    {
        private final String name;

        private MockAuthSubject( String name )
        {
            this.name = name;
        }

        @Override
        public void logout()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return name.equals( username );
        }

        @Override
        public String username()
            {
                return name;
            }
    }

    protected static class TestUserManagementProcedures extends UserManagementProcedures
    {
        private boolean failTerminateTransactions;

        void failTerminateTransaction()
        {
            failTerminateTransactions = true;
        }

        @Override
        protected void terminateTransactionsForValidUser( String username )
        {
            if ( failTerminateTransactions )
            {
                throw new RuntimeException( "Unexpected error" );
            }
        }

        @Override
        protected void terminateConnectionsForValidUser( String username )
        {
        }
    }
}
