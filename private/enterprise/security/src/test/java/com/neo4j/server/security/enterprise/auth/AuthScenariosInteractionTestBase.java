/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.Test;

import org.neo4j.test.DoubleLatch;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

public abstract class AuthScenariosInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{

    //---------- User creation -----------

    @Test
    void readOperationsShouldNotBeAllowedWhenPasswordChangeRequired() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        testFailRead( subject, pwdReqErrMsg( CHANGE_PWD_ERR_MSG ) );
    }

    @Test
    void passwordChangeShouldEnableRolePermissions() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        assertPasswordChangeWhenPasswordChangeRequired( subject, "bar", "foo" );
        subject = neo.login( "Henrik", "foo" );
        neo.assertAuthenticated( subject );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3 );
    }

    @Test
    void loginShouldFailWithIncorrectPassword() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "foo" );
        neo.assertUnauthenticated( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password (gets prompted to change - change to foo)
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to role Reader
    Henrik starts write transaction → permission denied
    Henrik starts read transaction → ok
    Henrik logs off
    */
    @Test
    void userCreation2() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        assertPasswordChangeWhenPasswordChangeRequired( subject, "bar", "foo" );
        subject = neo.login( "Henrik", "foo" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, ACCESS_DENIED );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to role Publisher
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → permission denied
    Henrik logs off
    */
    @Test
    void userCreation3() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, ACCESS_DENIED );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testFailSchema( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Henrik starts write transaction → permission denied
    Henrik starts schema transaction → permission denied
    Henrik creates user Craig → permission denied
    Admin adds user Henrik to role Architect
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → ok
    Henrik creates user Craig → permission denied
    Henrik logs off
    */
    @Test
    void userCreation4() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, ACCESS_DENIED );
        testFailWrite( subject, ACCESS_DENIED );
        testFailSchema( subject, ACCESS_DENIED );
        testFailCreateUser( subject, FAIL_EXECUTE_ADMIN_PROC );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + ARCHITECT + "', 'Henrik')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testSuccessfulSchema( subject );
        testFailCreateUser( subject, FAIL_EXECUTE_ADMIN_PROC );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik creates user Craig → permission denied
    Henrik logs off
     */
    @Test
    void userCreation5() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        testFailCreateUser( subject, FAIL_EXECUTE_ADMIN_PROC );
    }

    //---------- User deletion -----------

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Henrik logs in with correct password → fail
    */
    @Test
    void userDeletion1() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertUnauthenticated( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Admin adds user Henrik to role Publisher → fail
    */
    @Test
    void userDeletion2()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        assertSystemCommandFail( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')",
                String.format( "Failed to grant role '%s' to user '%s': User does not exist.", PUBLISHER, "Henrik" ) );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Admin deletes user Henrik
    Admin removes user Henrik from role Publisher → success, because idempotent operation
    */
    @Test
    void userDeletion3()
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    User Henrik logs in with correct password → ok
    Admin deletes user Henrik
    Henrik tries to login again → fail
    */
    @Test
    void userDeletion4() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        henrik = neo.login( "Henrik", "bar" );
        neo.assertUnauthenticated( henrik );
    }

    //---------- Role management -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik starts transaction with write query → ok
    Admin removes user Henrik from role Publisher
    Henrik starts transaction with read query → permission denied
    Admin adds Henrik to role Reader
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → ok
    */
    @Test
    void roleManagement1() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testSuccessfulWrite( subject );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
        testFailRead( subject, ACCESS_DENIED );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts transaction with write query → permission denied
    Admin adds user Henrik to role Publisher → ok
    Admin adds user Henrik to role Publisher → ok
    Henrik starts transaction with write query → ok
    */
    @Test
    void roleManagement2() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailWrite( subject, ACCESS_DENIED );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        testSuccessfulWrite( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Admin adds user Henrik to role Reader
    Henrik starts transaction with write query → ok
    Henrik starts transaction with read query → ok
    Admin removes user Henrik from role Publisher
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → ok
    */
    @Test
    void roleManagement3() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Admin adds user Henrik to role Reader
    Henrik starts transaction with write query → ok
    Henrik starts transaction with read query → ok
    Admin removes user Henrik from all roles
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → permission denied
     */
    @Test
    void roleManagement4() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + READER + "', 'Henrik')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
        testFailWrite( subject, ACCESS_DENIED );
        testFailRead( subject, ACCESS_DENIED );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik starts transaction with long running writing query Q
    Admin removes user Henrik from role Publisher (while Q still running)
    Q finishes and transaction is committed → ok
    Henrik starts new transaction with write query → permission denied
     */
    @Test
    void roleManagement5() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        write.executeCreateNode( threading, henrik );
        latch.startAndWaitForAllToStart();

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );

        latch.finishAndWaitForAllToFinish();

        write.closeAndAssertSuccess();
        testFailWrite( henrik, ACCESS_DENIED );
    }

    /*
     * Procedure 'test.allowedReadProcedure' with READ mode and 'allowed = role1' is loaded.
     * Procedure 'test.allowedWriteProcedure' with WRITE mode and 'allowed = role1' is loaded.
     * Procedure 'test.allowedSchemaProcedure' with SCHEMA mode and 'allowed = role1' is loaded.
     * Admin creates a new user 'mats'.
     * 'mats' logs in.
     * 'mats' executes the procedures, access denied.
     * Admin creates 'role1'.
     * 'mats' executes the procedures, access denied.
     * Admin adds role 'role1' to 'mats'.
     * 'mats' executes the procedures successfully.
     * Admin removes the role 'role1'.
     * 'mats' executes the procedures, access denied.
     * Admin creates the role 'role1' again (new).
     * 'mats' executes the procedures, access denied.
     * Admin adds role 'architect' to 'mats'.
     * 'mats' executes the procedures successfully.
     * Admin adds 'role1' to 'mats'.
     * 'mats' executes the procedures successfully.
     */
    @Test
    void customRoleWithProcedureAccess() throws Exception
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('mats', 'neo4j', false)" );
        S mats = neo.login( "mats", "neo4j" );
        testFailTestProcs( mats );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createRole('role1')" );
        grantAccess( "role1" );
        testFailTestProcs( mats );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('role1', 'mats')" );
        testSuccessfulTestProcs( mats );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.deleteRole('role1')" );
        testFailTestProcs( mats );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createRole('role1')" );
        testFailTestProcs( mats );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('architect', 'mats')" );
        testSuccessfulTestProcs( mats );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('role1', 'mats')" );
        testSuccessfulTestProcs( mats );
    }

    //---------- User suspension -----------

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password → ok
    Admin suspends user Henrik
    User Henrik logs in with correct password → fail
     */
    @Test
    void userSuspension1() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        neo.assertUnauthenticated( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Reader
    Henrik logs in with correct password → ok
    Henrik starts and completes transaction with read query → ok
    Admin suspends user Henrik
    Henrik logs in with correct password → fail
     */
    @Test
    void userSuspension2() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        neo.assertUnauthenticated( subject );
    }

    //---------- User activation -----------

    /*
    Admin creates user Henrik with password bar
    Admin suspends user Henrik
    Henrik logs in with correct password → fail
    Admin reinstates user Henrik
    Henrik logs in with correct password → ok
     */
    @Test
    void userActivation1() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertUnauthenticated( subject );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.activateUser('Henrik', false)" );
        subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
    }

    //---------- list users / roles -----------

    /*
    Admin lists all users → ok
    Admin creates user Henrik with password bar
    Admin lists all users → ok
    Henrik logs in with correct password → ok
    Henrik lists all users → permission denied
    Admin adds user Henrik to role Admin
    Henrik lists all users → ok
    */
    @Test
    void userListing() throws Throwable
    {
        testSuccessfulListUsers( adminSubject, initialUsers );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        testSuccessfulListUsers( adminSubject, with( initialUsers, "Henrik" ) );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListUsers( subject, FAIL_EXECUTE_ADMIN_PROC );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + ADMIN + "', 'Henrik')" );
        testSuccessfulListUsers( subject, with( initialUsers, "Henrik" ) );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password → ok
    Henrik lists all roles → permission denied
    Admin lists all roles → ok
    Admin adds user Henrik to role Admin
    Henrik lists all roles → ok
    */
    @Test
    void rolesListing() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListRoles( subject, FAIL_EXECUTE_ADMIN_PROC);
        testSuccessfulListRoles( adminSubject, initialRoles );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + ADMIN + "', 'Henrik')" );
        testSuccessfulListRoles( subject, initialRoles );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig
    Admin adds user Craig to role Publisher
    Henrik logs in with correct password → ok
    Henrik lists all roles for user Craig → permission denied
    Admin lists all roles for user Craig → ok
    Admin adds user Henrik to role Publisher
    */
    @Test
    void listingUserRoles() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Craig', 'foo', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Craig')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );

        testFailListUserRoles( subject, "Craig", "Permission denied for SHOW ROLE and/or SHOW USER." );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", PUBLISHER, PUBLIC ) );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig
    Admin adds user Henrik to role Publisher
    Admin adds user Craig to role Publisher
    Henrik logs in with correct password → ok
    Henrik lists all users for role Publisher → permission denied
    Admin lists all users for role Publisher → ok
    */
    @Test
    void listingRoleUsers() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Craig', 'foo', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Craig')" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListRoleUsers( subject, PUBLISHER, FAIL_EXECUTE_ADMIN_PROC );
        assertSystemCommandSuccess( adminSubject,
                "CALL dbms.security.listUsersForRole('" + PUBLISHER + "') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "Henrik", "Craig", "writeSubject" ) );
    }

    //---------- calling procedures -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password → ok
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → ok
    Admin adds user Henrik to role Reader
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → ok
    Admin removes Henrik from role Publisher
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → permission denied
     */
    @Test
    void callProcedures1() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        assertEmpty( henrik, "CALL test.createNode()" );
        assertSuccess( henrik, "CALL test.numNodes() YIELD count as count RETURN count",
                 r -> assertKeyIs( r, "count", "4" ) );

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );

        assertEmpty( henrik, "CALL test.createNode()" );
        assertSuccess( henrik, "CALL test.numNodes() YIELD count as count RETURN count",
                r -> assertKeyIs( r, "count", "5" ) );

        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );

        assertFail( henrik, "CALL test.createNode()",
                "Create node with labels '' is not allowed for user 'Henrik' with roles [PUBLIC, reader] restricted to TOKEN_WRITE." );
    }

    //---------- change password -----------

    /*
    Admin creates user Henrik with password abc
    Admin adds user Henrik to role Reader
    Henrik logs in with correct password → ok
    Henrik starts transaction with read query → ok
    Henrik changes password to 123
    Henrik starts transaction with read query → ok
    Henrik logs in with password abc → fail
    Henrik logs in with password 123 → ok
    Henrik starts transaction with read query → ok
    Henrik logs out
     */
    @Test
    void changeUserPassword1() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'abc', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertDDLCommandSuccess( subject, "ALTER CURRENT USER SET PASSWORD FROM 'abc' TO '123'" );
        neo.updateAuthToken( subject, "Henrik", "123" ); // Because RESTSubject caches an auth token that is sent with every request
        testSuccessfulRead( subject, 3 );
        subject = neo.login( "Henrik", "abc" );
        neo.assertUnauthenticated( subject );
        subject = neo.login( "Henrik", "123" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password abc
    Admin adds user Henrik to role Reader
    Henrik logs in with password abc → ok
    Henrik starts transaction with read query → ok
    Admin changes user Henrik’s password to 123
    Henrik logs in with password abc → fail
    Henrik logs in with password 123 → ok
    Henrik starts transaction with read query → ok
    Henrik logs out
     */
    @Test
    void changeUserPassword2() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'abc', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.changeUserPassword('Henrik', '123', false)" );
        subject = neo.login( "Henrik", "abc" );
        neo.assertUnauthenticated( subject );
        subject = neo.login( "Henrik", "123" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password abc
    Admin creates user Craig
    Admin adds user Henrik to role Reader
    Henrik logs in with password abc → ok
    Henrik starts transaction with read query → ok
    Henrik changes Craig’s password to 123 → fail
     */
    @Test
    void changeUserPassword3() throws Throwable
    {
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Craig', 'abc', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createUser('Henrik', 'abc', false)" );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertSystemCommandFail( subject, "CALL dbms.security.changeUserPassword('Craig', '123')", FAIL_EXECUTE_ADMIN_PROC );
    }

    // OTHER TESTS

    @Test
    void shouldNotTryToCreateTokensWhenReading()
    {
        assertEmpty( adminSubject, "CREATE (:MyNode)" );

        assertSuccess( readSubject, "MATCH (n:MyNode) WHERE n.nonExistent = 'foo' RETURN toString(count(*)) AS c", r -> assertKeyIs( r, "c", "0" ) );
        assertFail( readSubject, "MATCH (n:MyNode) SET n.nonExistent = 'foo' RETURN toString(count(*)) AS c", CREATE_PROPERTYKEY_OPS_NOT_ALLOWED );
        assertFail( readSubject, "MATCH (n:MyNode) SET n:Foo RETURN toString(count(*)) AS c", CREATE_LABEL_OPS_NOT_ALLOWED );
        assertSuccess( schemaSubject, "MATCH (n:MyNode) SET n.nonExistent = 'foo' RETURN toString(count(*)) AS c", r -> assertKeyIs( r, "c", "1" ) );
        assertSuccess( readSubject, "MATCH (n:MyNode) WHERE n.nonExistent = 'foo' RETURN toString(count(*)) AS c", r -> assertKeyIs( r, "c", "1" ) );
    }
}
