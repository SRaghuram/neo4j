/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.auth.AuthProcedures;
import org.neo4j.test.DoubleLatch;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.IS_SUSPENDED;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

public abstract class AuthProceduresInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{
    private static final String PWD_CHANGE = PASSWORD_CHANGE_REQUIRED.name().toLowerCase();

    //---------- General tests over all procedures -----------

    @Test
    void shouldHaveDescriptionsOnAllSecurityProcedures()
    {
        assertSuccess( readSubject, "CALL dbms.procedures", r ->
        {
            Stream<Map<String,Object>> securityProcedures = r.stream().filter( s ->
            {
                String name = s.get( "name" ).toString();
                String description = s.get( "description" ).toString();
                // TODO: remove filter for Transaction and Connection once those procedures are removed
                if ( name.contains( "dbms.security" ) &&
                     !(name.contains( "Transaction" ) || name.contains( "Connection" )) )
                {
                    assertThat( "Description for '" + name + "' should not be empty", description.trim().length(),
                            greaterThan( 0 ) );
                    return true;
                }
                return false;
            } );
            assertThat( securityProcedures.count(), equalTo( 15L ) );
        } );
    }

    //---------- Change own password -----------

    // Enterprise version of test in BuiltInProceduresIT.callChangePasswordWithAccessModeInDbmsMode.
    // Uses community edition procedure in BuiltInProcedures
    @Test
    void shouldChangeOwnPassword()
    {
        assertSystemCommandSuccess( readSubject, "ALTER CURRENT USER SET PASSWORD FROM '123' TO '321'" );
        // Because RESTSubject caches an auth token that is sent with every request
        neo.updateAuthToken( readSubject, "readSubject", "321" );
        neo.assertAuthenticated( readSubject );
        testSuccessfulRead( readSubject, 3 );
    }

    @Test
    void shouldChangeOwnPasswordEvenIfHasNoAuthorization()
    {
        neo.assertAuthenticated( noneSubject );
        assertSystemCommandSuccess( noneSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
        // Because RESTSubject caches an auth token that is sent with every request
        neo.updateAuthToken( noneSubject, "noneSubject", "321" );
        neo.assertAuthenticated( noneSubject );
    }

    @Test
    void shouldNotChangeOwnPasswordIfNewPasswordInvalid()
    {
        assertSystemCommandFail( readSubject, "ALTER CURRENT USER SET PASSWORD FROM '123' TO ''", "A password cannot be empty." );
        assertSystemCommandFail( readSubject, "ALTER CURRENT USER SET PASSWORD FROM '123' TO '123'",
                "Old password and new password cannot be the same." );
    }

    //---------- change user password -----------

    // Should change password for admin subject and valid user
    @Test
    void shouldChangeUserPassword() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321', false )" );
        // TODO: uncomment and fix
        // testUnAuthenticated( readSubject );

        neo.assertUnauthenticated( neo.login( "readSubject", "123" ) );
        neo.assertAuthenticated( neo.login( "readSubject", "321" ) );
    }

    @Test
    void shouldChangeUserPasswordAndRequirePasswordChangeOnNextLoginByDefault() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321' )" );
        neo.assertUnauthenticated( neo.login( "readSubject", "123" ) );
        neo.assertPasswordChangeRequired( neo.login( "readSubject", "321" ) );
    }

    @Test
    void shouldChangeUserPasswordAndRequirePasswordChangeOnNextLoginOnRequest() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321', true )" );
        neo.assertUnauthenticated( neo.login( "readSubject", "123" ) );
        neo.assertPasswordChangeRequired( neo.login( "readSubject", "321" ) );
    }

    // Should fail vaguely to change password for non-admin subject, regardless of user and password
    @Test
    void shouldNotChangeUserPasswordIfNotAdmin()
    {
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321' )", PERMISSION_DENIED );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.changeUserPassword( 'jake', '321' )", PERMISSION_DENIED );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '' )", PERMISSION_DENIED );
    }

    // Should change own password for non-admin or admin subject
    @Test
    void shouldChangeUserPasswordIfSameUser()
    {
        assertSystemCommandSuccess( readSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321', false )" );
        // Because RESTSubject caches an auth token that is sent with every request
        neo.updateAuthToken( readSubject, "readSubject", "321" );
        neo.assertAuthenticated( readSubject );
        testSuccessfulRead( readSubject, 3 );

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.changeUserPassword( 'adminSubject', 'cba', false )" );
        // Because RESTSubject caches an auth token that is sent with every request
        neo.updateAuthToken( adminSubject, "adminSubject", "cba" );
        neo.assertAuthenticated( adminSubject );
        testSuccessfulRead( adminSubject, 3 );
    }

    // Should fail nicely to change own password for non-admin or admin subject if password invalid
    @Test
    void shouldFailToChangeUserPasswordIfSameUserButInvalidPassword()
    {
        assertSystemCommandFail( readSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '123' )",
                "Old password and new password cannot be the same." );

        assertSystemCommandFail( adminSubject, "CALL dbms.security.changeUserPassword( 'adminSubject', 'abc' )",
                "Old password and new password cannot be the same." );
    }

    // Should fail nicely to change password for admin subject and non-existing user
    @Test
    void shouldNotChangeUserPasswordIfNonExistentUser()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.changeUserPassword( 'jake', '321' )",
                "Failed to alter the specified user 'jake': User does not exist." );
    }

    // Should fail nicely to change password for admin subject and empty password
    @Test
    void shouldNotChangeUserPasswordIfEmptyPassword()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '' )",
                "A password cannot be empty." );
    }

    // Should fail to change password for admin subject and same password
    @Test
    void shouldNotChangeUserPasswordIfSamePassword()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '123' )",
                "Old password and new password cannot be the same." );
    }

    //---------- create user -----------

    @Test
    void shouldCreateUserAndRequirePasswordChangeByDefault() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('craig', '1234' )" );
        userManager.getUser( "craig" );
        neo.assertUnauthenticated( neo.login( "craig", "321" ) );
        neo.assertPasswordChangeRequired( neo.login( "craig", "1234" ) );
    }

    @Test
    void shouldCreateUserAndRequirePasswordChangeIfRequested() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('craig', '1234', true)" );
        userManager.getUser( "craig" );
        neo.assertUnauthenticated( neo.login( "craig", "321" ) );
        neo.assertPasswordChangeRequired( neo.login( "craig", "1234" ) );
    }

    @Test
    void shouldCreateUserAndRequireNoPasswordChangeIfRequested() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('craig', '1234', false)" );
        userManager.getUser( "craig" );
        neo.assertAuthenticated( neo.login( "craig", "1234" ) );
    }

    @Test
    void shouldNotCreateUserIfInvalidUsername()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser(null, '1234', true)",
                "The provided username is empty." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser('', '1234', true)",
                "The provided username is empty." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser(',ss!', '1234', true)",
                "Username ',ss!' contains illegal characters." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser(',ss!', '', true)",
                "Username ',ss!' contains illegal characters." );
    }

    @Test
    void shouldNotCreateUserIfInvalidPassword()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser('craig', '', true)", "A password cannot be empty." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser('craig', null, true)", "A password cannot be empty." );
    }

    @Test
    void shouldNotCreateExistingUser()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser('readSubject', '1234', true)",
                "The specified user 'readSubject' already exists" );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createUser('readSubject', '', true)",
                "A password cannot be empty." );
    }

    @Test
    void shouldNotAllowNonAdminCreateUser()
    {
        testFailCreateUser( pwdSubject, CHANGE_PWD_ERR_MSG );
        testFailCreateUser( readSubject, PERMISSION_DENIED );
        testFailCreateUser( writeSubject, PERMISSION_DENIED );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
    }

    //---------- delete user -----------

    @Test
    void shouldDeleteUser() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteUser('noneSubject')" );
        try
        {
            userManager.getUser( "noneSubject" );
            fail( "User noneSubject should not exist" );
        }
        catch ( InvalidArgumentsException e )
        {
            assertTrue( e.getMessage().contains( "User 'noneSubject' does not exist." ),
                    "User noneSubject should not exist" );
        }

        userManager.addRoleToUser( PUBLISHER, "readSubject" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('readSubject')" );
        try
        {
            userManager.getUser( "readSubject" );
            fail( "User readSubject should not exist" );
        }
        catch ( InvalidArgumentsException e )
        {
            assertTrue( e.getMessage().contains( "User 'readSubject' does not exist." ),
                    "User readSubject should not exist" );
        }
        assertFalse( userManager.getUsernamesForRole( READER ).contains( "readSubject" ) );
        assertFalse( userManager.getUsernamesForRole( PUBLISHER ).contains( "readSubject" ) );
    }

    @Test
    void shouldNotDeleteUserIfNotAdmin()
    {
        testFailDeleteUser( pwdSubject, "readSubject", CHANGE_PWD_ERR_MSG );
        testFailDeleteUser( readSubject, "readSubject", PERMISSION_DENIED );
        testFailDeleteUser( writeSubject, "readSubject", PERMISSION_DENIED );

        testFailDeleteUser( schemaSubject, "readSubject", PERMISSION_DENIED );
        testFailDeleteUser( schemaSubject, "Craig", PERMISSION_DENIED );
        testFailDeleteUser( schemaSubject, "", PERMISSION_DENIED );
    }

    @Test
    void shouldNotAllowDeletingNonExistentUser()
    {
        testFailDeleteUser( adminSubject, "Craig", "Failed to delete the specified user 'Craig': User does not exist." );
        testFailDeleteUser( adminSubject, "", "Failed to delete the specified user '': User does not exist." );
    }

    @Test
    void shouldNotAllowDeletingYourself()
    {
        testFailDeleteUser( adminSubject, "adminSubject", "Deleting yourself (user 'adminSubject') is not allowed." );
    }

    @Test
    void shouldTerminateTransactionsOnUserDeletion() throws Throwable
    {
        shouldTerminateTransactionsForUser( writeSubject, "dbms.security.deleteUser( '%s' )" );
    }

    @Test
    void shouldTerminateConnectionsOnUserDeletion() throws Exception
    {
        TransportConnection conn = startBoltSession( "writeSubject", "abc" );

        Map<String,Long> boltConnections = countBoltConnectionsByUsername();
        assertThat( boltConnections.get( "writeSubject" ), equalTo( IS_EMBEDDED ? 1L : 2L ) );

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteUser( 'writeSubject' )" );

        boltConnections = countBoltConnectionsByUsername();
        assertThat( boltConnections.get( "writeSubject" ), equalTo( null ) );

        conn.disconnect();
    }

    //---------- suspend user -----------

    @Test
    void shouldSuspendUser() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('readSubject')" );
        assertTrue( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    void shouldSuspendSuspendedUser() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('readSubject')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('readSubject')" );
        assertTrue( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    void shouldFailToSuspendNonExistentUser()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.suspendUser('Craig')", "User 'Craig' does not exist." );
    }

    @Test
    void shouldFailToSuspendIfNotAdmin()
    {
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.suspendUser('readSubject')", PERMISSION_DENIED );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.suspendUser('Craig')", PERMISSION_DENIED );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.suspendUser('')", PERMISSION_DENIED );
    }

    @Test
    void shouldFailToSuspendYourself()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.suspendUser('adminSubject')",
                "Suspending yourself (user 'adminSubject') is not allowed." );
    }

    @Test
    void shouldTerminateTransactionsOnUserSuspension() throws Throwable
    {
        shouldTerminateTransactionsForUser( writeSubject, "dbms.security.suspendUser( '%s' )" );
    }

    @Test
    void shouldTerminateConnectionsOnUserSuspension() throws Exception
    {
        TransportConnection conn = startBoltSession( "writeSubject", "abc" );

        Map<String,Long> boltConnections = countBoltConnectionsByUsername();
        assertThat( boltConnections.get( "writeSubject" ), equalTo( IS_EMBEDDED ? 1L : 2L ) );

        assertEmpty( adminSubject, "CALL dbms.security.suspendUser( 'writeSubject' )" );

        boltConnections = countBoltConnectionsByUsername();
        assertThat( boltConnections.get( "writeSubject" ), equalTo( null ) );

        conn.disconnect();
    }

    //---------- activate user -----------

    @Test
    void shouldActivateUserAndRequirePasswordChangeByDefault() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.activateUser('readSubject')" );
        neo.assertUnauthenticated( neo.login( "readSubject", "321" ) );
        neo.assertPasswordChangeRequired( neo.login( "readSubject", "123" ) );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    void shouldActivateUserAndRequirePasswordChangeIfRequested() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.activateUser('readSubject', true)" );
        neo.assertUnauthenticated( neo.login( "readSubject", "321" ) );
        neo.assertPasswordChangeRequired( neo.login( "readSubject", "123" ) );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    void shouldActivateUserAndRequireNoPasswordChangeIfRequested() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.activateUser('readSubject', false)" );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    void shouldActivateActiveUser() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.activateUser('readSubject')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.activateUser('readSubject')" );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    void shouldFailToActivateNonExistentUser()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.activateUser('Craig')", "User 'Craig' does not exist." );
    }

    @Test
    void shouldFailToActivateIfNotAdmin() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.activateUser('readSubject')", PERMISSION_DENIED );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.activateUser('Craig')", PERMISSION_DENIED );
        assertSystemCommandFail( schemaSubject, "CALL dbms.security.activateUser('')", PERMISSION_DENIED );
    }

    @Test
    void shouldFailToActivateYourself()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.activateUser('adminSubject')",
                "Activating yourself (user 'adminSubject') is not allowed." );
    }

    //---------- add user to role -----------

    @Test
    void shouldAddRoleToUser() throws Exception
    {
        assertFalse( userHasRole( "readSubject", PUBLISHER ), "Should not have role publisher" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'readSubject' )" );
        assertTrue( userHasRole( "readSubject", PUBLISHER ), "Should have role publisher" );
    }

    @Test
    void shouldAddRetainUserInRole() throws Exception
    {
        assertTrue( userHasRole( "readSubject", READER ), "Should have role reader" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'readSubject')" );
        assertTrue( userHasRole( "readSubject", READER ), "Should have still have role reader" );
    }

    @Test
    void shouldFailToAddNonExistentUserToRole()
    {
        testFailAddRoleToUser( adminSubject, PUBLISHER, "Olivia", "User 'Olivia' does not exist." );
        testFailAddRoleToUser( adminSubject, "thisRoleDoesNotExist", "Olivia", "User 'Olivia' does not exist." );
        testFailAddRoleToUser( adminSubject, "", "Olivia", "The provided role name is empty." );
    }

    @Test
    void shouldFailToAddUserToNonExistentRole()
    {
        testFailAddRoleToUser( adminSubject, "thisRoleDoesNotExist", "readSubject",
                "Role 'thisRoleDoesNotExist' does not exist." );
        testFailAddRoleToUser( adminSubject, "", "readSubject", "The provided role name is empty." );
    }

    @Test
    void shouldFailToAddRoleToUserIfNotAdmin()
    {
        testFailAddRoleToUser( pwdSubject, PUBLISHER, "readSubject", CHANGE_PWD_ERR_MSG );
        testFailAddRoleToUser( readSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );
        testFailAddRoleToUser( writeSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );

        testFailAddRoleToUser( schemaSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );
        testFailAddRoleToUser( schemaSubject, PUBLISHER, "Olivia", PERMISSION_DENIED );
        testFailAddRoleToUser( schemaSubject, "thisRoleDoesNotExist", "Olivia", PERMISSION_DENIED );
    }

    //---------- remove user from role -----------

    @Test
    void shouldRemoveRoleFromUser() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + READER + "', 'readSubject')" );
        assertFalse( userHasRole( "readSubject", READER ),  "Should not have role reader" );
    }

    @Test
    void shouldKeepUserOutOfRole() throws Exception
    {
        assertFalse( userHasRole( "readSubject", PUBLISHER ), "Should not have role publisher" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'readSubject')" );
        assertFalse( userHasRole( "readSubject", PUBLISHER ), "Should not have role publisher" );
    }

    @Test
    void shouldFailToRemoveUserFromNonExistentRole()
    {
        testFailRemoveRoleFromUser( adminSubject, "thisRoleDoesNotExist", "readSubject",
                "Role 'thisRoleDoesNotExist' does not exist." );
        testFailRemoveRoleFromUser( adminSubject, "", "readSubject", "The provided role name is empty." );
    }

    @Test
    void shouldFailToRemoveRoleFromUserIfNotAdmin()
    {
        testFailRemoveRoleFromUser( pwdSubject, PUBLISHER, "readSubject", CHANGE_PWD_ERR_MSG );
        testFailRemoveRoleFromUser( readSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );
        testFailRemoveRoleFromUser( writeSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );

        testFailRemoveRoleFromUser( schemaSubject, READER, "readSubject", PERMISSION_DENIED );
        testFailRemoveRoleFromUser( schemaSubject, READER, "Olivia", PERMISSION_DENIED );
        testFailRemoveRoleFromUser( schemaSubject, "thisRoleDoesNotExist", "Olivia", PERMISSION_DENIED );
    }

    //---------- manage multiple roles -----------

    @Test
    void shouldAllowAddingAndRemovingUserFromMultipleRoles() throws Exception
    {
        assertFalse( userHasRole( "readSubject", PUBLISHER ), "Should not have role publisher" );
        assertFalse( userHasRole( "readSubject", ARCHITECT ), "Should not have role architect" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'readSubject')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + ARCHITECT + "', 'readSubject')" );
        assertTrue( userHasRole( "readSubject", PUBLISHER ), "Should have role publisher" );
        assertTrue( userHasRole( "readSubject", ARCHITECT ), "Should have role architect" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'readSubject')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + ARCHITECT + "', 'readSubject')" );
        assertFalse( userHasRole( "readSubject", PUBLISHER ), "Should not have role publisher" );
        assertFalse( userHasRole( "readSubject", ARCHITECT ), "Should not have role architect" );
    }

    //---------- create role -----------

    @Test
    void shouldCreateRole() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createRole('new_role')" );
        userManager.assertRoleExists( "new_role" );
        assertEquals( userManager.getUsernamesForRole( "new_role" ).size(), 0 );
    }

    @Test
    void shouldNotCreateRoleIfInvalidRoleName()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createRole('')", "The provided role name is empty." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createRole('&%ss!')",
                "Role name '&%ss!' contains illegal characters. Use simple ascii characters and numbers." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createRole('åäöø')",
                "Role name 'åäöø' contains illegal characters. Use simple ascii characters and numbers" );
    }

    @Test
    void shouldNotCreateExistingRole()
    {
        assertSystemCommandFail( adminSubject, format( "CALL dbms.security.createRole('%s')", ARCHITECT ),
                "The specified role 'architect' already exists" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createRole('new_role')" );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.createRole('new_role')",
                "The specified role 'new_role' already exists" );
    }

    @Test
    void shouldNotAllowNonAdminCreateRole()
    {
        testFailCreateRole( pwdSubject, CHANGE_PWD_ERR_MSG );
        testFailCreateRole( readSubject, PERMISSION_DENIED );
        testFailCreateRole( writeSubject, PERMISSION_DENIED );
        testFailCreateRole( schemaSubject, PERMISSION_DENIED );
    }

    //---------- delete role -----------

    @Test
    void shouldThrowIfNonAdminTryingToDeleteRole()
    {
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.createRole('%s')", "new_role" ) );
        testFailDeleteRole( schemaSubject, "new_role", PERMISSION_DENIED );
        testFailDeleteRole( writeSubject, "new_role", PERMISSION_DENIED );
        testFailDeleteRole( readSubject, "new_role", PERMISSION_DENIED );
        testFailDeleteRole( noneSubject, "new_role", PERMISSION_DENIED );
    }

    @Test
    void shouldThrowIfDeletingNonExistentRole()
    {
        testFailDeleteRole( adminSubject, "nonExistent", "Failed to delete the specified role 'nonExistent': Role does not exist." );
    }

    @Test
    void shouldDeleteRole() throws Exception
    {
        neo.getLocalUserManager().newRole( "new_role" );
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.deleteRole('%s')", "new_role" ) );

        assertThat( userManager.getAllRoleNames(), not( contains( "new_role" ) ) );
    }

    @Test
    void shouldDeletePredefinedRoles()
    {
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.deleteRole('%s')", READER ) );
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.deleteRole('%s')", ARCHITECT ) );

        assertThat( userManager.getAllRoleNames(), equalTo( Set.of( ADMIN, EDITOR, PUBLISHER, EMPTY_ROLE ) ) );
    }

    @Test
    void shouldLoseAdminRightsWhenAdminRoleIsDeleted()
    {
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.deleteRole('%s')", ADMIN ) );

        assertSystemCommandFail( adminSubject, format( "CALL dbms.security.deleteRole('%s')", PUBLISHER ), "Permission denied" );

        assertThat( userManager.getAllRoleNames(), equalTo( Set.of( READER, EDITOR, PUBLISHER, ARCHITECT, EMPTY_ROLE ) ) );
    }

    @Test
    void deletingRoleAssignedToSelfShouldWork() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.createRole('%s')", "new_role" ) );
        assertSystemCommandSuccess( adminSubject,
                format( "CALL dbms.security.addRoleToUser('%s', '%s')", "new_role", "adminSubject" ) );
        assertThat( userManager.getRoleNamesForUser( "adminSubject" ), hasItem( "new_role" ) );

        assertSystemCommandSuccess( this.adminSubject, format( "CALL dbms.security.deleteRole('%s')", "new_role" ) );
        assertThat( userManager.getRoleNamesForUser( "adminSubject" ), not( hasItem( "new_role" ) ) );
        assertThat( userManager.getAllRoleNames(), not( contains( "new_role" ) ) );
    }

    //---------- list users -----------

    @Test
    void checkUserResultClassesHaveSameFieldsInCommunityAndEnterprise()
    {
        Field[] communityFields = AuthProcedures.UserResult.class.getFields();
        Field[] enterpriseFields = AuthProceduresBase.UserResult.class.getFields();
        assertEquals( communityFields.length, enterpriseFields.length );

        for ( int i = 0; i < communityFields.length; i++ )
        {
            Field comField = communityFields[i];
            Field entField = enterpriseFields[i];

            assertTrue( Modifier.isFinal( comField.getModifiers() ) );
            assertTrue( Modifier.isFinal( entField.getModifiers() ) );
            assertEquals( comField.getName(), entField.getName() );
        }
    }

    @Test
    void shouldListUsers()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyIs( r, "username", initialUsers ) );
    }

    @Test
    void shouldReturnUsersWithRoles() throws Exception
    {
        Map<String,Object> expected = map(
                "adminSubject", listOf( ADMIN ),
                "readSubject", listOf( READER ),
                "schemaSubject", listOf( ARCHITECT ),
                "writeSubject", listOf( READER, PUBLISHER ),
                "editorSubject", listOf( EDITOR ),
                "pwdSubject", listOf(),
                "noneSubject", listOf(),
                "neo4j", listOf( ADMIN )
        );
        userManager.addRoleToUser( READER, "writeSubject" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( expected ) ) );
    }

    @Test
    void shouldReturnUsersWithFlags() throws Exception
    {
        Map<String,Object> expected = map(
                "adminSubject", listOf(),
                "readSubject", listOf(),
                "schemaSubject", listOf(),
                "editorSubject", listOf(),
                "writeSubject", listOf( IS_SUSPENDED ),
                "pwdSubject", listOf( PWD_CHANGE, IS_SUSPENDED ),
                "noneSubject", listOf(),
                "neo4j", listOf( PWD_CHANGE.toLowerCase() )
        );
        userManager.suspendUser( "writeSubject" );
        userManager.suspendUser( "pwdSubject" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "flags", valueOf( expected ) ) );
    }

    @Test
    void shouldShowCurrentUser() throws Exception
    {
        userManager.addRoleToUser( READER, "writeSubject" );
        assertSuccess( adminSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( map( "adminSubject", listOf( ADMIN ) ) ) ) );
        assertSuccess( readSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( map( "readSubject", listOf( READER ) ) ) ) );
        assertSuccess( schemaSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( map( "schemaSubject", listOf( ARCHITECT ) ) ) ) );
        assertSuccess( writeSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles",
                        valueOf( map( "writeSubject", listOf( READER, PUBLISHER ) ) ) ) );
        assertSuccess( noneSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( map( "noneSubject", listOf() ) ) ) );
    }

    @Test
    void shouldNotAllowNonAdminListUsers()
    {
        testFailListUsers( pwdSubject, CHANGE_PWD_ERR_MSG );
        testFailListUsers( readSubject, PERMISSION_DENIED );
        testFailListUsers( writeSubject, PERMISSION_DENIED );
        testFailListUsers( schemaSubject, PERMISSION_DENIED );
    }

    //---------- list roles -----------

    @Test
    void shouldListRoles()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listRoles() YIELD role",
                r -> assertKeyIs( r, "role", initialRoles ) );
    }

    @Test
    void shouldReturnRolesWithUsers()
    {
        Map<String,Object> expected = map(
                ADMIN, listOf( "adminSubject", "neo4j" ),
                READER, listOf( "readSubject" ),
                ARCHITECT, listOf( "schemaSubject" ),
                PUBLISHER, listOf( "writeSubject" ),
                EDITOR, listOf( "editorSubject" ),
                "empty", listOf()
        );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listRoles()",
                r -> assertKeyIsMap( r, "role", "users", valueOf( expected ) ) );
    }

    @Test
    void shouldNotAllowNonAdminListRoles()
    {
        testFailListRoles( pwdSubject, CHANGE_PWD_ERR_MSG );
        testFailListRoles( readSubject, PERMISSION_DENIED );
        testFailListRoles( writeSubject, PERMISSION_DENIED );
        testFailListRoles( schemaSubject, PERMISSION_DENIED );
    }

    //---------- list roles for user -----------

    @Test
    void shouldListRolesForUser()
    {
        assertSystemCommandSuccess( adminSubject,
                "CALL dbms.security.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", ADMIN ) );
        assertSystemCommandSuccess( adminSubject,
                "CALL dbms.security.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READER ) );
    }

    @Test
    void shouldListNoRolesForUserWithNoRoles()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listRolesForUser('Henrik') YIELD value as roles RETURN roles" );
    }

    @Test
    void shouldNotListRolesForNonExistentUser()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.listRolesForUser('Petra') YIELD value as roles RETURN roles",
                "User 'Petra' does not exist." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.listRolesForUser('') YIELD value as roles RETURN roles",
                "User '' does not exist." );
    }

    @Test
    void shouldListOwnRolesRoles()
    {
        assertSystemCommandSuccess( adminSubject,
                "CALL dbms.security.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", ADMIN ) );
        assertSystemCommandSuccess( readSubject,
                "CALL dbms.security.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READER ) );
    }

    @Test
    void shouldNotAllowNonAdminListUserRoles()
    {
        testFailListUserRoles( pwdSubject, "adminSubject", CHANGE_PWD_ERR_MSG );
        testFailListUserRoles( readSubject, "adminSubject", PERMISSION_DENIED );
        testFailListUserRoles( writeSubject, "adminSubject", PERMISSION_DENIED );
        testFailListUserRoles( schemaSubject, "adminSubject", PERMISSION_DENIED );
    }

    //---------- list users for role -----------

    @Test
    void shouldListUsersForRole()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsersForRole('admin') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "adminSubject", "neo4j" ) );
    }

    @Test
    void shouldListNoUsersForRoleWithNoUsers()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsersForRole('empty') YIELD value as users RETURN users" );
    }

    @Test
    void shouldNotListUsersForNonExistentRole()
    {
        assertSystemCommandFail( adminSubject, "CALL dbms.security.listUsersForRole('poodle') YIELD value as users RETURN users",
                "Role 'poodle' does not exist." );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.listUsersForRole('') YIELD value as users RETURN users",
                "Role '' does not exist." );
    }

    @Test
    void shouldNotListUsersForRoleIfNotAdmin()
    {
        testFailListRoleUsers( pwdSubject, ADMIN, CHANGE_PWD_ERR_MSG );
        testFailListRoleUsers( readSubject, ADMIN, PERMISSION_DENIED );
        testFailListRoleUsers( writeSubject, ADMIN, PERMISSION_DENIED );
        testFailListRoleUsers( schemaSubject, ADMIN, PERMISSION_DENIED );
    }

    //---------- clearing authentication cache -----------

    @Test
    void shouldAllowClearAuthCacheIfAdmin()
    {
        assertEmpty( adminSubject, "CALL dbms.security.clearAuthCache()" );
    }

    @Test
    void shouldNotClearAuthCacheIfNotAdmin()
    {
        assertFail( pwdSubject, "CALL dbms.security.clearAuthCache()", CHANGE_PWD_ERR_MSG );
        assertFail( readSubject, "CALL dbms.security.clearAuthCache()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.security.clearAuthCache()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.clearAuthCache()", PERMISSION_DENIED );
    }

    //---------- permissions -----------

    @Test
    void shouldPrintUserAndRolesWhenPermissionDenied() throws Throwable
    {
        userManager.newUser( "mats", password( "foo" ), false );
        userManager.newRole( "failer", "mats" );
        S mats = neo.login( "mats", "foo" );

        assertFail( noneSubject, "CALL test.numNodes",
                "Read operations are not allowed for user 'noneSubject' with no roles." );
        assertFail( readSubject, "CALL test.allowedWriteProcedure",
                "Write operations are not allowed for user 'readSubject' with roles [reader]." );
        assertFail( writeSubject, "CALL test.allowedSchemaProcedure",
                "Schema operations are not allowed for user 'writeSubject' with roles [publisher]." );
        assertFail( mats, "CALL test.numNodes",
                "Read operations are not allowed for user 'mats' with roles [failer]." );
        // UDFs
        assertFail( mats, "RETURN test.allowedFunction1()",
                "Read operations are not allowed for user 'mats' with roles [failer]." );
    }

    @Test
    void shouldAllowProcedureStartingTransactionInNewThread()
    {
        ClassWithProcedures.exceptionsInProcedure.clear();
        DoubleLatch latch = new DoubleLatch( 2 );
        ClassWithProcedures.doubleLatch = latch;
        latch.start();
        assertEmpty( writeSubject, "CALL test.threadTransaction" );
        latch.finishAndWaitForAllToFinish();
        assertThat( ClassWithProcedures.exceptionsInProcedure.size(), equalTo( 0 ) );
        assertSuccess( adminSubject, "MATCH (:VeryUniqueLabel) RETURN toString(count(*)) as n",
                r -> assertKeyIs( r, "n", "1" ) );
    }

    @Test
    void shouldInheritSecurityContextWhenProcedureStartingTransactionInNewThread()
    {
        ClassWithProcedures.exceptionsInProcedure.clear();
        DoubleLatch latch = new DoubleLatch( 2 );
        ClassWithProcedures.doubleLatch = latch;
        latch.start();
        assertEmpty( readSubject, "CALL test.threadReadDoingWriteTransaction" );
        latch.finishAndWaitForAllToFinish();
        assertThat( ClassWithProcedures.exceptionsInProcedure.size(), equalTo( 1 ) );
        assertThat( ClassWithProcedures.exceptionsInProcedure.get( 0 ).getMessage(), containsString( WRITE_OPS_NOT_ALLOWED ) );
                assertSuccess( adminSubject, "MATCH (:VeryUniqueLabel) RETURN toString(count(*)) as n",
                r -> assertKeyIs( r, "n", "0" ) );
    }

    @Test
    void shouldSetCorrectUnAuthenticatedPermissions() throws Throwable
    {
        S unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "MATCH (n) RETURN n", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "CREATE (:Node)", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "CREATE INDEX ON :Node(number)", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertSystemCommandFail( unknownUser, "ALTER CURRENT USER SET PASSWORD FROM 'Matban' TO '321'", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertSystemCommandFail( unknownUser, "CALL dbms.security.createUser('Henrik', 'bar', true)", "" );
    }

    @Test
    void shouldSetCorrectPasswordChangeRequiredPermissions() throws Throwable
    {
        testFailRead( pwdSubject, pwdReqErrMsg( PERMISSION_DENIED ) );
        testFailWrite( pwdSubject, pwdReqErrMsg( PERMISSION_DENIED ) );
        testFailSchema( pwdSubject, pwdReqErrMsg( PERMISSION_DENIED ) );
        assertPasswordChangeWhenPasswordChangeRequired( pwdSubject, "abc", "321" );

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + ARCHITECT + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( henrik );
        testFailRead( henrik, pwdReqErrMsg( PERMISSION_DENIED ) );
        testFailWrite( henrik, pwdReqErrMsg( PERMISSION_DENIED ) );
        testFailSchema( henrik, pwdReqErrMsg( PERMISSION_DENIED ) );
        assertPasswordChangeWhenPasswordChangeRequired( henrik, "bar", "321" );

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Olivia', 'bar', true)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + ADMIN + "', 'Olivia')" );
        S olivia = neo.login( "Olivia", "bar" );
        neo.assertPasswordChangeRequired( olivia );
        testFailRead( olivia, pwdReqErrMsg( PERMISSION_DENIED ) );
        testFailWrite( olivia, pwdReqErrMsg( PERMISSION_DENIED ) );
        testFailSchema( olivia, pwdReqErrMsg( PERMISSION_DENIED ) );
        assertSystemCommandFail( olivia, "CALL dbms.security.createUser('OliviasFriend', 'bar', false)", CHANGE_PWD_ERR_MSG );
        assertPasswordChangeWhenPasswordChangeRequired( olivia, "bar", "321" );
    }

    @Test
    void shouldSetCorrectNoRolePermissions()
    {
        testFailRead( noneSubject );
        testFailWrite( noneSubject );
        testFailSchema( noneSubject );
        testFailCreateUser( noneSubject, PERMISSION_DENIED );
        assertSystemCommandSuccess( noneSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
    }

    @Test
    void shouldSetCorrectReaderPermissions()
    {
        testSuccessfulRead( readSubject, 3 );
        testFailWrite( readSubject );
        testFailTokenWrite( readSubject, WRITE_OPS_NOT_ALLOWED );
        testFailSchema( readSubject );
        testFailCreateUser( readSubject, PERMISSION_DENIED );
        assertSystemCommandSuccess( readSubject, "ALTER CURRENT USER SET PASSWORD FROM '123' TO '321'" );
    }

    @Test
    void shouldSetCorrectEditorPermissions()
    {
        testSuccessfulRead( editorSubject, 3 );
        testSuccessfulWrite( editorSubject );
        testFailTokenWrite( editorSubject );
        testFailSchema( editorSubject );
        testFailCreateUser( editorSubject, PERMISSION_DENIED );
        assertSystemCommandSuccess( editorSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
    }

    @Test
    void shouldSetCorrectPublisherPermissions()
    {
        testSuccessfulRead( writeSubject, 3 );
        testSuccessfulWrite( writeSubject );
        testSuccessfulTokenWrite( writeSubject );
        testFailSchema( writeSubject );
        testFailCreateUser( writeSubject, PERMISSION_DENIED );
        assertSystemCommandSuccess( writeSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
    }

    @Test
    void shouldSetCorrectSchemaPermissions()
    {
        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulTokenWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
        assertSystemCommandSuccess( schemaSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
    }

    @Test
    void shouldSetCorrectAdminPermissions()
    {
        testSuccessfulRead( adminSubject, 3 );
        testSuccessfulWrite( adminSubject );
        testSuccessfulTokenWrite( adminSubject );
        testSuccessfulSchema( adminSubject );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Olivia', 'bar', true)" );
        assertSystemCommandSuccess( adminSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
    }

    @Test
    void shouldSetCorrectMultiRolePermissions()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'schemaSubject')" );

        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
        assertSystemCommandSuccess( schemaSubject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '321'" );
    }
}
