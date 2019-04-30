/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.InMemorySystemGraphOperations;

import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphInitializer;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.test.assertion.Assert.assertException;

class UserManagementProceduresLoggingTest
{
    private TestUserManagementProcedures authProcedures;
    private AssertableLogProvider log;
    private CommercialSecurityContext matsContext;
    private EnterpriseUserManager userManager;

    @BeforeEach
    void setUp() throws Throwable
    {
        SecureHasher secureHasher = new SecureHasher();
        log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        SystemGraphImportOptions importOptions =
                new SystemGraphImportOptions( false, false, true, false, InMemoryUserRepository::new, InMemoryRoleRepository::new, InMemoryUserRepository::new,
                        InMemoryRoleRepository::new, InMemoryUserRepository::new, InMemoryUserRepository::new );

        InMemorySystemGraphOperations ops = new InMemorySystemGraphOperations( secureHasher );
        SystemGraphInitializer graphInitializer =
                new SystemGraphInitializer( mock( QueryExecutor.class ), ops, importOptions, secureHasher, mock( Log.class ) );
        userManager = new SystemGraphRealm( ops, graphInitializer, true, secureHasher, new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() ), true, true );
        ((SystemGraphRealm) userManager).start();

        authProcedures = new TestUserManagementProcedures();
        authProcedures.graph = mock( GraphDatabaseAPI.class );
        authProcedures.securityLog = securityLog;

        CommercialSecurityContext adminContext =
                new CommercialSecurityContext( new MockAuthSubject( "admin" ), AccessMode.Static.FULL, Collections.emptySet(), true );
        matsContext =
                new CommercialSecurityContext( new MockAuthSubject( "mats" ), AccessMode.Static.NONE, Collections.emptySet(), false );

        setSubject( adminContext );
        log.clear();
    }

    private void setSubject( CommercialSecurityContext securityContext )
    {
        authProcedures.securityContext = securityContext;
        authProcedures.userManager = new PersonalUserManager( userManager, securityContext.subject(),
                authProcedures.securityLog, securityContext.isAdmin() );
    }

    @Test
    void shouldLogCreatingUser() throws Throwable
    {
        authProcedures.createUser( "andres", "el password", true );
        authProcedures.createUser( "mats", "el password", false );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "andres", ", with password change required" ),
                info( "[admin]: created user `%s`%s", "mats", "" )
            );
    }

    @Test
    void shouldLogFailureToCreateUser()
    {
        catchInvalidArguments( () -> authProcedures.createUser( null, "pw", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "", "pw", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "andres", "", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "mats", null, true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "neo4j", "nonEmpty", true ) );

        log.assertExactly(
                error( "[admin]: tried to create user `%s`: %s", null, "The provided username is empty." ),
                error( "[admin]: tried to create user `%s`: %s", "", "The provided username is empty." ),
                error( "[admin]: tried to create user `%s`: %s", "andres", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "mats", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "neo4j", "The specified user 'neo4j' already exists." )
        );
    }

    @Test
    void shouldLogUnauthorizedCreatingUser()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.createUser( "andres", "", true ) );

        log.assertExactly( error( "[mats]: tried to create user `%s`: %s", "andres", PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogDeletingUser() throws Throwable
    {
        authProcedures.createUser( "andres", "el password", false );
        authProcedures.deleteUser( "andres" );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "andres", "" ),
                info( "[admin]: deleted user `%s`", "andres" ) );
    }

    @Test
    void shouldLogDeletingNonExistentUser()
    {
        catchInvalidArguments( () -> authProcedures.deleteUser( "andres" ) );

        log.assertExactly( error( "[admin]: tried to delete user `%s`: %s", "andres", "User 'andres' does not exist." ) );
    }

    @Test
    void shouldLogUnauthorizedDeleteUser()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.deleteUser( ADMIN ) );

        log.assertExactly( error( "[mats]: tried to delete user `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogAddingRoleToUser() throws Throwable
    {
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( ARCHITECT, "mats" );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "mats", "" ),
                info( "[admin]: added role `%s` to user `%s`", ARCHITECT, "mats" ) );
    }

    @Test
    void shouldLogFailureToAddRoleToUser() throws Throwable
    {
        authProcedures.createUser( "mats", "neo4j", false );
        catchInvalidArguments( () -> authProcedures.addRoleToUser( "null", "mats" ) );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "mats", "" ),
                error( "[admin]: tried to add role `%s` to user `%s`: %s", "null", "mats", "Role 'null' does not exist." ) );
    }

    @Test
    void shouldLogUnauthorizedAddingRole()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.addRoleToUser( ADMIN, "mats" ) );

        log.assertExactly( error( "[mats]: tried to add role `%s` to user `%s`: %s", ADMIN, "mats", PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogRemovalOfRoleFromUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( READER, "mats" );
        log.clear();

        // When
        authProcedures.removeRoleFromUser( READER, "mats" );

        // Then
        log.assertExactly( info( "[admin]: removed role `%s` from user `%s`", READER, "mats" ) );
    }

    @Test
    void shouldLogFailureToRemoveRoleFromUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( READER, "mats" );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.removeRoleFromUser( "notReader", "mats" ) );
        catchInvalidArguments( () -> authProcedures.removeRoleFromUser( READER, "notMats" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to remove role `%s` from user `%s`: %s", "notReader", "mats", "Role 'notReader' does not exist." ),
                error( "[admin]: tried to remove role `%s` from user `%s`: %s", READER, "notMats", "User 'notMats' does not exist." )
        );
    }

    @Test
    void shouldLogUnauthorizedRemovingRole()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.removeRoleFromUser( ADMIN, ADMIN ) );

        log.assertExactly( error( "[mats]: tried to remove role `%s` from user `%s`: %s", ADMIN, ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogUserPasswordChanges() throws IOException, InvalidArgumentsException
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", true );
        log.clear();

        // When
        authProcedures.changeUserPassword( "mats", "longPassword", false );
        authProcedures.changeUserPassword( "mats", "longerPassword", true );

        setSubject( matsContext );
        authProcedures.changeUserPassword( "mats", "evenLongerPassword", false );

        authProcedures.changePassword( "superLongPassword", false );
        authProcedures.changePassword( "infinitePassword", true );

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
    void shouldLogFailureToChangeUserPassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "andres", "neo4j", true );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "andres", "neo4j", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "andres", "", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "notAndres", "good password", false ) );

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
    void shouldLogFailureToChangeOwnPassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", true );
        setSubject( matsContext );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "mats", "neo4j", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "mats", "", false ) );

        catchInvalidArguments( () -> authProcedures.changePassword( null, false ) );
        catchInvalidArguments( () -> authProcedures.changePassword( "", false ) );
        catchInvalidArguments( () -> authProcedures.changePassword( "neo4j", false ) );

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
    void shouldLogUnauthorizedChangePassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "andres", "neo4j", true );
        log.clear();
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.changeUserPassword( "andres", "otherPw", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change password for user `%s`: %s", "andres", PERMISSION_DENIED )
        );
    }

    @Test
    void shouldLogSuspendUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        log.clear();

        // When
        authProcedures.suspendUser( "mats" );
        authProcedures.suspendUser( "mats" );

        // Then
        log.assertExactly(
                info( "[admin]: suspended user `%s`", "mats" ),
                info( "[admin]: suspended user `%s`", "mats" )
        );
    }

    @Test
    void shouldLogFailureToSuspendUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.suspendUser( "notMats" ) );
        catchInvalidArguments( () -> authProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to suspend user `%s`: %s", "notMats", "User 'notMats' does not exist." ),
                error( "[admin]: tried to suspend user `%s`: %s", "admin", "Suspending yourself (user 'admin') is not allowed." )
        );
    }

    @Test
    void shouldLogUnauthorizedSuspendUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to suspend user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    @Test
    void shouldLogActivateUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.suspendUser( "mats" );
        log.clear();

        // When
        authProcedures.activateUser( "mats", false );
        authProcedures.activateUser( "mats", false );

        // Then
        log.assertExactly(
                info( "[admin]: activated user `%s`", "mats" ),
                info( "[admin]: activated user `%s`", "mats" )
        );
    }

    @Test
    void shouldLogFailureToActivateUser()
    {
        // When
        catchInvalidArguments( () -> authProcedures.activateUser( "notMats", false ) );
        catchInvalidArguments( () -> authProcedures.activateUser( ADMIN, false ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to activate user `%s`: %s", "notMats", "User 'notMats' does not exist." ),
                error( "[admin]: tried to activate user `%s`: %s", ADMIN, "Activating yourself (user 'admin') is not allowed." )
        );
    }

    @Test
    void shouldLogUnauthorizedActivateUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.activateUser( "admin", true ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to activate user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    @Test
    void shouldLogCreatingRole() throws Throwable
    {
        // When
        authProcedures.createRole( "role" );

        // Then
        log.assertExactly( info( "[admin]: created role `%s`", "role" ) );
    }

    @Test
    void shouldLogFailureToCreateRole() throws Throwable
    {
        // Given
        authProcedures.createRole( "role" );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.createRole( null ) );
        catchInvalidArguments( () -> authProcedures.createRole( "" ) );
        catchInvalidArguments( () -> authProcedures.createRole( "role" ) );
        catchInvalidArguments( () -> authProcedures.createRole( "!@#$" ) );

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
    void shouldLogUnauthorizedCreateRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.createRole( "role" ) );

        // Then
        log.assertExactly( error("[mats]: tried to create role `%s`: %s", "role", PERMISSION_DENIED) );
    }

    @Test
    void shouldLogDeletingRole() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        log.clear();

        // When
        authProcedures.deleteRole( "foo" );

        // Then
        log.assertExactly( info( "[admin]: deleted role `%s`", "foo" ) );
    }

    @Test
    void shouldLogFailureToDeleteRole()
    {
        // When
        catchInvalidArguments( () -> authProcedures.deleteRole( null ) );
        catchInvalidArguments( () -> authProcedures.deleteRole( "" ) );
        catchInvalidArguments( () -> authProcedures.deleteRole( "foo" ) );
        catchInvalidArguments( () -> authProcedures.deleteRole( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to delete role `%s`: %s", null, "Role 'null' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", "", "Role '' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", "foo", "Role 'foo' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", ADMIN, "'admin' is a predefined role and can not be deleted or modified." )
        );
    }

    @Test
    void shouldLogUnauthorizedDeletingRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.deleteRole( ADMIN ) );

        // Then
        log.assertExactly( error( "[mats]: tried to delete role `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogGrantPrivilege() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        log.clear();

        // When
        authProcedures.grantPrivilegeToRole( "foo", "read", "graph", "*" );

        // Then
        log.assertExactly( info( "[admin]: granted `%s` privilege on `%s` for database `%s` to role `%s`", Action.READ, "graph", "*", "foo" ) );
    }

    @Test
    void shouldLogFailureToGrantPrivilege() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        log.clear();

        // When
        InvalidArgumentsException exception =
                assertThrows( InvalidArgumentsException.class, () -> authProcedures.grantPrivilegeToRole( "bar", "read", "graph", "*" ) );
        assertThat( exception.getMessage(), equalTo( "Role 'bar' does not exist." ) );
        exception = assertThrows( InvalidArgumentsException.class, () -> authProcedures.grantPrivilegeToRole( "foo", "bar", "graph", "*" ) );
        assertThat( exception.getMessage(), equalTo( "'bar' is not a valid action" ) );
        exception = assertThrows( InvalidArgumentsException.class, () -> authProcedures.grantPrivilegeToRole( "foo", "write", "bar", "*" ) );
        assertThat( exception.getMessage(), equalTo( "'bar' is not a valid resource" ) );
        exception = assertThrows( InvalidArgumentsException.class, () -> authProcedures.grantPrivilegeToRole( "foo", "execute", "schema", "*" ) );
        assertThat( exception.getMessage(), equalTo( "Schema resource cannot be combined with action 'execute'" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to grant `%s` privilege on `%s` for database `%s` to role `%s`: %s",
                        Action.READ, "graph", "*", "bar", "Role 'bar' does not exist." )
        );
    }

    @Test
    void shouldLogUnauthorizedGrantPrivilege() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        setSubject( matsContext );
        log.clear();

        // When
        catchAuthorizationViolation( () -> authProcedures.grantPrivilegeToRole( "foo", "write", "graph", "*" ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to grant `%s` privilege on `%s` for database `%s` to role `%s`: %s",
                        Action.WRITE, "graph", "*", "foo", "Permission denied." )
        );
    }

    @Test
    void shouldLogRevokePrivilege() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        log.clear();

        // When
        authProcedures.revokePrivilegeFromRole( "foo", "read", "graph", "*" );

        // Then
        log.assertExactly( info( "[admin]: revoked `%s` privilege on `%s` for database `%s` from role `%s`", Action.READ, "graph", "*", "foo" ) );
    }

    @Test
    void shouldLogFailureToRevokePrivilege() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        log.clear();

        // When
        InvalidArgumentsException exception =
                assertThrows( InvalidArgumentsException.class, () -> authProcedures.revokePrivilegeFromRole( "bar", "read", "graph", "*" ) );
        assertThat( exception.getMessage(), equalTo( "Role 'bar' does not exist." ) );
        exception = assertThrows( InvalidArgumentsException.class, () -> authProcedures.revokePrivilegeFromRole( "foo", "bar", "graph", "*" ) );
        assertThat( exception.getMessage(), equalTo( "'bar' is not a valid action" ) );
        exception = assertThrows( InvalidArgumentsException.class, () -> authProcedures.revokePrivilegeFromRole( "foo", "write", "bar", "*" ) );
        assertThat( exception.getMessage(), equalTo( "'bar' is not a valid resource" ) );
        exception = assertThrows( InvalidArgumentsException.class, () -> authProcedures.revokePrivilegeFromRole( "foo", "execute", "schema", "*" ) );
        assertThat( exception.getMessage(), equalTo( "Schema resource cannot be combined with action 'execute'" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to revoke `%s` privilege on `%s` for database `%s` from role `%s`: %s",
                        Action.READ, "graph", "*", "bar", "Role 'bar' does not exist." )
        );
    }

    @Test
    void shouldLogUnauthorizedRevokePrivilege() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        setSubject( matsContext );
        log.clear();

        // When
        catchAuthorizationViolation( () -> authProcedures.revokePrivilegeFromRole( "foo", "write", "graph", "*" ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to revoke `%s` privilege on `%s` for database `%s` from role `%s`: %s",
                        Action.WRITE, "graph", "*", "foo", "Permission denied." )
        );
    }

    @Test
    void shouldLogIfUnexpectedErrorTerminatingTransactions() throws Exception
    {
        // Given
        authProcedures.createUser( "johan", "neo4j", false );
        authProcedures.failTerminateTransaction();
        log.clear();

        // When
        assertException( () -> authProcedures.deleteUser( "johan" ), RuntimeException.class, "Unexpected error" );

        // Then
        log.assertExactly(
                info( "[admin]: deleted user `%s`", "johan" ),
                error( "[admin]: failed to terminate running transaction and bolt connections for user `%s` following %s: %s",
                        "johan", "deletion", "Unexpected error" )
        );
    }

    @Test
    void shouldLogUnauthorizedListUsers()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listUsers() );

        log.assertExactly( error( "[mats]: tried to list users: %s", PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogUnauthorizedListRoles()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listRoles() );

        log.assertExactly( error( "[mats]: tried to list roles: %s", PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogFailureToListRolesForUser()
    {
        // Given

        // When
        catchInvalidArguments( () -> authProcedures.listRolesForUser( null ) );
        catchInvalidArguments( () -> authProcedures.listRolesForUser( "" ) );
        catchInvalidArguments( () -> authProcedures.listRolesForUser( "nonExistent" ) );

        log.assertExactly(
                error( "[admin]: tried to list roles for user `%s`: %s", null, "User 'null' does not exist." ),
                error( "[admin]: tried to list roles for user `%s`: %s", "", "User '' does not exist." ),
                error( "[admin]: tried to list roles for user `%s`: %s", "nonExistent", "User 'nonExistent' does not exist." )
        );
    }

    @Test
    void shouldLogUnauthorizedListRolesForUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listRolesForUser( "user" ) );

        log.assertExactly( error( "[mats]: tried to list roles for user `%s`: %s", "user", PERMISSION_DENIED ) );
    }

    @Test
    void shouldLogFailureToListUsersForRole()
    {
        // Given

        // When
        catchInvalidArguments( () -> authProcedures.listUsersForRole( null ) );
        catchInvalidArguments( () -> authProcedures.listUsersForRole( "" ) );
        catchInvalidArguments( () -> authProcedures.listUsersForRole( "nonExistent" ) );

        log.assertExactly(
                error( "[admin]: tried to list users for role `%s`: %s", null, "Role 'null' does not exist." ),
                error( "[admin]: tried to list users for role `%s`: %s", "", "Role '' does not exist." ),
                error( "[admin]: tried to list users for role `%s`: %s", "nonExistent", "Role 'nonExistent' does not exist." )
        );
    }

    @Test
    void shouldLogUnauthorizedListUsersForRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listUsersForRole( "role" ) );

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

    private AssertableLogProvider.LogMatcher info( String message, Object... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, Object... arguments )
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
