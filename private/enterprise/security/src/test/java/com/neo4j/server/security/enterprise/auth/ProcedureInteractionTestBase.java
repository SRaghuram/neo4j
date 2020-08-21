/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.ThreadingExtension;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;
import static org.neo4j.procedure.Mode.WRITE;

@ExtendWith( {TestDirectorySupportExtension.class, ThreadingExtension.class} )
public abstract class ProcedureInteractionTestBase<S>
{
    static final Matcher<Object> ONE_AS_INT = equalTo( 1 );
    static final Matcher<Object> ONE_AS_LONG = equalTo( 1L );
    static final Matcher<Object> TWO_AS_INT = equalTo( 2 );
    static final Matcher<Object> TWO_AS_LONG = equalTo( 2L );
    static final String SCHEMA_OPS_NOT_ALLOWED = "Schema operations are not allowed";
    private static final String PROCEDURE_TIMEOUT_ERROR = "Procedure got: Transaction guard check failed";
    String CHANGE_PWD_ERR_MSG = AuthorizationViolationException.PERMISSION_DENIED;
    private static final String BOLT_PWD_ERR_MSG =
            "The credentials you provided were valid, but must be changed before you can use this instance.";
    String PERMISSION_DENIED = "Permission denied.";
    String ACCESS_DENIED = "Database access is not allowed for user";
    String CREATE_LABEL_OPS_NOT_ALLOWED = "Creating new node label is not allowed";
    String CREATE_RELTYPE_OPS_NOT_ALLOWED = "Creating new relationship type is not allowed";
    String CREATE_PROPERTYKEY_OPS_NOT_ALLOWED = "Creating new property name is not allowed";

    protected boolean IS_EMBEDDED = true;

    String pwdReqErrMsg( String errMsg )
    {
        return IS_EMBEDDED ? errMsg : BOLT_PWD_ERR_MSG;
    }

    final String EMPTY_ROLE = "empty";

    S adminSubject;
    S schemaSubject;
    S writeSubject;
    S editorSubject;
    S readSubject;
    S pwdSubject;
    S noneSubject;

    String[] initialUsers = {"adminSubject", "readSubject", "schemaSubject",
            "writeSubject", "editorSubject", "pwdSubject", "noneSubject", "neo4j"};
    String[] initialRoles = {ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER, EMPTY_ROLE, PUBLIC};

    @Inject
    public TestDirectory testDirectory;

    @Inject
    ThreadingRule threading;

    protected NeoInteractionLevel<S> neo;
    protected final TransportTestUtil util = new TransportTestUtil();
    Path securityLog;

    Map<Setting<?>,String> defaultConfiguration()
    {
        Path homeDir = testDirectory.directoryPath( "logs" );
        securityLog = homeDir.resolve( "security.log" );
        return Map.of(
                GraphDatabaseSettings.logs_directory, homeDir.toAbsolutePath().toString(),
                GraphDatabaseSettings.procedure_roles,
                "test.allowed*Procedure:role1;test.nestedAllowedFunction:role1;" + "test.allowedFunc*:role1;test.*estedAllowedProcedure:role1"
        );
    }

    @SuppressWarnings( "rawtypes" )
    Set<Class> defaultProcedures()
    {
        Set<Class> procedureClasses = new HashSet<>();
        procedureClasses.add( ClassWithProcedures.class );
        return procedureClasses;
    }

    @BeforeEach
    public void setUp( TestInfo testInfo ) throws Throwable
    {
        configuredSetup( defaultConfiguration(), testInfo );
    }

    void configuredSetup( Map<Setting<?>,String> config, TestInfo testInfo ) throws Throwable
    {
        neo = setUpNeoServer( config, testInfo );
        GlobalProcedures globalProcedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class );
        for ( var procClass : defaultProcedures() )
        {
            globalProcedures.registerProcedure( procClass );
        }
        globalProcedures.registerFunction( ClassWithFunctions.class );

        newUser( "noneSubject", "abc", false );
        newUser( "pwdSubject", "abc", true );
        newUser( "adminSubject", "abc", false );
        newUser( "schemaSubject", "abc", false );
        newUser( "writeSubject", "abc", false );
        newUser( "editorSubject", "abc", false );
        newUser( "readSubject", "123", false );

        grantRoleToUser( ADMIN, "adminSubject" );
        grantRoleToUser( ARCHITECT, "schemaSubject" );
        grantRoleToUser( PUBLISHER, "writeSubject" );
        grantRoleToUser( EDITOR, "editorSubject" );
        grantRoleToUser( READER, "readSubject" );

        neo.clearPublicRole();

        noneSubject = neo.login( "noneSubject", "abc" );
        pwdSubject = neo.login( "pwdSubject", "abc" );
        readSubject = neo.login( "readSubject", "123" );
        editorSubject = neo.login( "editorSubject", "abc" );
        writeSubject = neo.login( "writeSubject", "abc" );
        schemaSubject = neo.login( "schemaSubject", "abc" );
        adminSubject = neo.login( "adminSubject", "abc" );
        createRoleWithAccess( EMPTY_ROLE );
        setupTokensAndNodes();
    }

    private void newUser( String username, String password, boolean pwdChangeRequired )
    {
        String query;

        if ( pwdChangeRequired )
        {
            query = "CREATE USER %s SET PASSWORD '%s'";
        }
        else
        {
            query = "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED";
        }

        authDisabledAdminstrationCommand( String.format(query, username, password) );
    }

    private void grantRoleToUser( String role, String user )
    {
        authDisabledAdminstrationCommand( String.format( "GRANT ROLE %s TO %s", role, user ) );
    }

    void authDisabledAdminstrationCommand( String query )
    {
        try ( Transaction tx = neo.getSystemGraph().beginTx() )
        {
            tx.execute( query );
            tx.commit();
        }
    }

    private void setupTokensAndNodes()
    {
        // Note: the intention of this query is to seed the token store with labels and property keys
        // that an editor should be allowed to use.
        assertEmpty( schemaSubject, "CREATE (n) SET n:A:Test:NEWNODE:VeryUniqueLabel:Node " +
                "SET n.id = '2', n.square = '4', n.name = 'me', n.prop = 'a', n.number = '1' " +
                "DELETE n" );

        assertEmpty( writeSubject, "UNWIND range(0,2) AS number CREATE (:Node {number:number, name:'node'+number})" );
    }

    protected abstract NeoInteractionLevel<S> setUpNeoServer( Map<Setting<?>,String> config, TestInfo testInfo ) throws Throwable;

    @AfterEach
    public void tearDown() throws Throwable
    {
        if ( neo != null )
        {
            neo.tearDown();
        }
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream( strs ), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    List<String> listOf( String... values )
    {
        return Stream.of( values ).collect( toList() );
    }

    //------------- Helper functions---------------

    void testSuccessfulRead( S subject, Object count )
    {
        assertSuccess( subject, "MATCH (n) RETURN count(n) as count, 1 AS ignored", r ->
        {
            List<Object> result = r.stream().map( s -> s.get( "count" ) ).collect( toList() );
            assertThat( result.size() ).isEqualTo( 1 );
            assertThat( result.get( 0 ) ).isEqualTo( valueOf( count ) );
        } );
    }

    void testFailRead( S subject, String errMsg )
    {
        assertFail( subject, "MATCH (n) RETURN count(n)", errMsg );
    }

    void testSuccessfulWrite( S subject )
    {
        assertEmpty( subject, "CREATE (:Node)" );
    }

    void testFailWrite( S subject )
    {
        testFailWrite( subject, "Create node with labels 'Node' is not allowed" );
    }

    void testFailWrite( S subject, String errMsg )
    {
        assertFail( subject, "CREATE (:Node)", errMsg );
    }

    void testSuccessfulTokenWrite( S subject )
    {
        assertEmpty( subject, "CALL db.createLabel('NewNodeName')" );
    }

    void testFailTokenWrite( S subject )
    {
        testFailTokenWrite( subject, CREATE_LABEL_OPS_NOT_ALLOWED );
    }

    void testFailTokenWrite( S subject, String errMsg )
    {
        assertFail( subject, "CALL db.createLabel('NewNodeName')", errMsg );
    }

    void testSuccessfulSchema( S subject )
    {
        assertEmpty( subject, "CREATE INDEX FOR (n:Node) ON (n.number)" );
    }

    void testFailSchema( S subject )
    {
        testFailSchema( subject, SCHEMA_OPS_NOT_ALLOWED );
    }

    void testFailSchema( S subject, String errMsg )
    {
        assertFail( subject, "CREATE INDEX FOR (n:Node) ON (n.number)", errMsg );
    }

    void testFailCreateUser( S subject, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.createUser('Craig', 'foo', false)", errMsg );
        assertSystemCommandFail( subject, "CALL dbms.security.createUser('Craig', '', false)", errMsg );
        assertSystemCommandFail( subject, "CALL dbms.security.createUser('', 'foo', false)", errMsg );
    }

    void testFailCreateRole( S subject, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.createRole('RealAdmins')", errMsg );
        assertSystemCommandFail( subject, "CALL dbms.security.createRole('RealAdmins')", errMsg );
        assertSystemCommandFail( subject, "CALL dbms.security.createRole('RealAdmins')", errMsg );
    }

    void testFailAddRoleToUser( S subject, String role, String username, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.addRoleToUser('" + role + "', '" + username + "')", errMsg );
    }

    void testFailRemoveRoleFromUser( S subject, String role, String username, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.removeRoleFromUser('" + role + "', '" + username + "')", errMsg );
    }

    void testFailDeleteUser( S subject, String username, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.deleteUser('" + username + "')", errMsg );
    }

    void testFailDeleteRole( S subject, String roleName, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.deleteRole('" + roleName + "')", errMsg );
    }

    @SuppressWarnings( {"SameParameterValue"} )
    boolean userIsSuspended( String username )
    {
        List<Object> suspendedUsers = new LinkedList<>();
        neo.executeQuery( adminSubject, SYSTEM_DATABASE_NAME, "SHOW USERS", null,
                r -> r.stream().filter( u -> u.get( "suspended" ).equals( valueOf( true ) ) ).map( u -> u.get( "user" ) ).forEach( suspendedUsers::add ) );
        return suspendedUsers.contains( valueOf( username ) );
    }

    // List users

    void testSuccessfulListUsers( S subject, Object[] users )
    {
        assertSystemCommandSuccess( subject, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyIsArray( r, "username", users ) );
    }

    void testFailListUsers( S subject, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.listUsers() YIELD username", errMsg );
    }

    @SuppressWarnings( "SameParameterValue" )
    void testListUsersContains( S subject, Object user )
    {
        assertSystemCommandSuccess( subject, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyContains( r, "username", user ) );
    }

    void testListUsersNotContains( S subject, Object user )
    {
        assertSystemCommandSuccess( subject, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyNotContains( r, "username", user ) );
    }

    // List roles

    void testSuccessfulListRoles( S subject, Object[] roles )
    {
        assertSystemCommandSuccess( subject, "CALL dbms.security.listRoles() YIELD role",
                r -> assertKeyIsArray( r, "role", roles ) );
    }

    void testFailListRoles( S subject, String errMsg )
    {
        assertSystemCommandFail( subject, "CALL dbms.security.listRoles() YIELD role", errMsg );
    }

    @SuppressWarnings( "SameParameterValue" )
    void testListRolesContains( S subject, Object role )
    {
        assertSystemCommandSuccess( subject, "CALL dbms.security.listRoles() YIELD role",
                r -> assertKeyContains( r, "role", role ) );
    }

    // List roles for specific user

    void testFailListUserRoles( S subject, String username, String errMsg )
    {
        assertSystemCommandFail( subject,
                "CALL dbms.security.listRolesForUser('" + username + "') YIELD value AS roles RETURN count(roles)",
                errMsg );
    }

    void testListUserRolesContains( S subject, String username, String roleName )
    {
        assertSystemCommandSuccess( subject,
                "CALL dbms.security.listRolesForUser('" + username + "') YIELD value AS roles",
                r -> assertKeyContains( r, "roles", roleName ));
    }

    void testListUserRolesNotContains( S subject, String username, String roleName )
    {
        assertSystemCommandSuccess( subject,
                "CALL dbms.security.listRolesForUser('" + username + "') YIELD value AS roles",
                r -> assertKeyNotContains( r, "roles", roleName ));
    }

    // List users for specific role

    void testFailListRoleUsers( S subject, String role, String errMsg )
    {
        assertSystemCommandFail( subject,
                "CALL dbms.security.listUsersForRole('" + role + "') YIELD value AS users RETURN count(users)",
                errMsg );
    }

    @SuppressWarnings( "SameParameterValue" )
    void testListRoleUsersNotContains( S subject, String role, String username )
    {
        assertSystemCommandSuccess( subject,
                "CALL dbms.security.listUsersForRole('" + role + "') YIELD value AS users",
                r -> assertKeyNotContains( r, "users", username ));
    }

    void testFailTestProcs( S subject )
    {
        assertFail( subject, "CALL test.allowedReadProcedure()", ACCESS_DENIED );
        assertFail( subject, "CALL test.allowedWriteProcedure()", ACCESS_DENIED );
        assertFail( subject, "CALL test.allowedSchemaProcedure()", ACCESS_DENIED );
    }

    void testSuccessfulTestProcs( S subject )
    {
        assertSuccess( subject, "CALL test.allowedReadProcedure()",
                r -> assertKeyIs( r, "value", "foo" ) );
        assertSuccess( subject, "CALL test.allowedWriteProcedure()",
                r -> assertKeyIs( r, "value", "a", "a" ) );
        assertSuccess( subject, "CALL test.allowedSchemaProcedure()",
                r -> assertKeyIs( r, "value", "OK" ) );
    }

    void assertPasswordChangeWhenPasswordChangeRequired( S subject, String oldPassword, String newPassword )
    {
        S subjectToUse;
        String query;

        // remove if-else ASAP
        if ( IS_EMBEDDED )
        {
            subjectToUse = subject;
            query = format( "ALTER CURRENT USER SET PASSWORD FROM '%s' TO '%s'", oldPassword, newPassword );
        }
        else
        {
            subjectToUse = adminSubject;
            query = format( "ALTER USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", neo.nameOf( subject ), newPassword );
        }
        assertDDLCommandSuccess( subjectToUse, query );
    }

    void assertFail( S subject, String call, String partOfErrorMsg )
    {
        assertFail( DEFAULT_DATABASE_NAME, subject, call, partOfErrorMsg );
    }

    @SuppressWarnings( "SameParameterValue" )
    void assertFail( String database, S subject, String call, String partOfErrorMsg )
    {
        String err = assertCallEmpty( subject, database, call, null );
        if ( StringUtils.isEmpty( err ) )
        {
            fail( format( "Should have failed with an error message containing: %s", partOfErrorMsg ) );
        }
        else
        {
            assertThat( err ).contains( partOfErrorMsg );
        }
    }

    void assertSystemCommandSuccess( S subject, String call )
    {
        String err = assertCallEmpty( subject, SYSTEM_DATABASE_NAME, call, null );
        assertThat( err ).isEqualTo( "" );
    }

    void assertDDLCommandSuccess( S subject, String query )
    {
        neo.executeQuery( subject, SYSTEM_DATABASE_NAME, query, null, result -> assertFalse( result.hasNext() ) );
    }

    void assertSystemCommandSuccess( S subject, String call, Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        String err = neo.executeQuery( subject, SYSTEM_DATABASE_NAME, call, null, resultConsumer );
        assertThat( err ).isEqualTo( "" );
    }

    @SuppressWarnings( "SameParameterValue" )
    void assertSystemCommandFail( S subject, String call, String partOfErrorMsg )
    {
        String err = assertCallEmpty( subject, SYSTEM_DATABASE_NAME, call, null );
        if ( StringUtils.isEmpty( err ) )
        {
            fail( format( "Should have failed with an error message containing: %s", partOfErrorMsg ) );
        }
        else
        {
            assertThat( err ).contains( partOfErrorMsg );
        }
    }

    void assertEmpty( S subject, String call )
    {
        String err = assertCallEmpty( subject, DEFAULT_DATABASE_NAME, call, null );
        assertThat( err ).isEqualTo( "" );
    }

    void assertSuccess( S subject, String call, Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        assertSuccess( subject, DEFAULT_DATABASE_NAME, call, null, resultConsumer );
    }

    void assertSuccess( S subject, String database, String call, Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        assertSuccess( subject, database, call, null, resultConsumer );
    }

    void assertSuccess( S subject, String call, Map<String, Object> params, Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        assertSuccess( subject, DEFAULT_DATABASE_NAME, call, params, resultConsumer );
    }

    private void assertSuccess( S subject, String database, String call, Map<String,Object> params,
                                Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        String err = neo.executeQuery( subject, database, call, params, resultConsumer );
        assertThat( err ).isEqualTo( "" );
    }

    @SuppressWarnings( "SameParameterValue" )
    List<Map<String,Object>> collectSuccessResult( S subject, String call )
    {
        List<Map<String,Object>> result = new LinkedList<>();
        assertSuccess( subject, call, r -> r.stream().forEach( result::add ) );
        return result;
    }

    @SuppressWarnings( "SameParameterValue" )
    private String assertCallEmpty( S subject, String database, String call, Map<String, Object> params )
    {
        return neo.executeQuery( subject, database, call, params,
                result ->
                {
                    List<Map<String,Object>> collect = result.stream().collect( toList() );
                    assertTrue( collect.isEmpty(), "Expected no results but got: " + collect );
                } );
    }

    void createRole( String roleName, String... usernames )
    {
        createRole( roleName, false, usernames );
    }

    void createRoleWithAccess( String roleName, String... usernames )
    {
        createRole( roleName, true, usernames );
    }

    private void createRole( String roleName, boolean grantAccess, String... usernames )
    {
        assertDDLCommandSuccess( adminSubject, format( "CREATE ROLE %s", roleName) );
        if ( grantAccess )
        {
            grantAccess( roleName );
        }

        for ( String username : usernames )
        {
            assertDDLCommandSuccess( adminSubject, format( "GRANT ROLE %s TO %s", roleName, username) );
        }
    }

    void grantAccess( String roleName )
    {
        assertDDLCommandSuccess( adminSubject, format( "GRANT ACCESS ON DATABASE * TO %s", roleName ) );
    }

    List<Object> getObjectsAsList( ResourceIterator<Map<String,Object>> r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( toList() );
    }

    private void assertKeyContains( ResourceIterator<Map<String,Object>> r, String key, Object item )
    {
        List<Object> results = getObjectsAsList( r, key );
        assertTrue( results.contains( valueOf( item )) );
    }

    private void assertKeyNotContains( ResourceIterator<Map<String,Object>> r, String key, Object item )
    {
        List<Object> results = getObjectsAsList( r, key );
        assertFalse( results.contains( valueOf( item ) ) );
    }

    void assertKeyIs( ResourceIterator<Map<String,Object>> r, String key, Object... items )
    {
        assertKeyIsArray( r, key, items );
    }

    private void assertKeyIsArray( ResourceIterator<Map<String,Object>> r, String key, Object[] items )
    {
        List<Object> results = getObjectsAsList( r, key );
        assertEquals( Arrays.asList( items ).size(), results.size(), "Didn't get expected number of results" );
        assertThat( results ).contains( Arrays.stream( items ).map( this::valueOf ).toArray() );
    }

    @SuppressWarnings( "unchecked" )
    static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            Object expected )
    {
        if ( expected instanceof MapValue )
        {
            assertKeyIsMap( r, keyKey, valueKey, (MapValue) expected );
        }
        else
        {
            assertKeyIsMap( r, keyKey, valueKey, (Map<String,Object>) expected );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            Map<String,Object> expected )
    {
        List<Map<String,Object>> result = r.stream().collect( toList() );

        assertEquals( expected.size(), result.size(), "Results for should have size " + expected.size() + " but was " + result.size() );

        for ( Map<String,Object> row : result )
        {
            String key = (String) row.get( keyKey );
            assertThat( expected ).containsKey( key );
            assertThat( row ).containsKey( valueKey );

            Object objectValue = row.get( valueKey );
            if ( objectValue instanceof List )
            {
                List<String> value = (List<String>) objectValue;
                List<String> expectedValues = (List<String>) expected.get( key );
                assertEquals( value.size(), expectedValues.size(), "sizes" );
                assertThat( value ).containsAll( expectedValues );
            }
            else
            {
                String value = objectValue.toString();
                String expectedValue = expected.get( key ).toString();
                assertThat( value ).isEqualTo( expectedValue );
            }
        }
    }

    private static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            MapValue expected )
    {
        List<Map<String,Object>> result = r.stream().collect( toList() );

        assertEquals( expected.size(), result.size(), "Results for should have size " + expected.size() + " but was " + result.size() );

        for ( Map<String,Object> row : result )
        {
            TextValue key = (TextValue) row.get( keyKey );
            assertTrue( expected.containsKey( key.stringValue() ) );
            assertThat( row ).containsKey( valueKey );

            Object objectValue = row.get( valueKey );
            if ( objectValue instanceof ListValue )
            {
                ListValue value = (ListValue) objectValue;
                ListValue expectedValues = (ListValue) expected.get( key.stringValue() );
                assertEquals( expectedValues.size(), value.size(), "sizes" );
                assertThat( Arrays.asList( value.asArray() ) ).contains( expectedValues.asArray() );
            }
            else
            {
                String value = ((TextValue) objectValue).stringValue();
                String expectedValue = ((TextValue) expected.get( key.stringValue() )).stringValue();
                assertThat( value ).isEqualTo( expectedValue );
            }
        }
    }

    // --------------------- helpers -----------------------

    Object toRawValue( Object value )
    {
        if ( value instanceof AnyValue )
        {
            BaseToObjectValueWriter<RuntimeException> writer = writer();
            ((AnyValue) value).writeTo( writer );
            return writer.value();
        }
        else
        {
            return value;
        }
    }

    public static class CountResult
    {
        public final String count;

        CountResult( Long count )
        {
            this.count = "" + count;
        }
    }

    public static final class Support implements AutoCloseable
    {
        private static final AtomicLong COUNTER = new AtomicLong();
        private static final ConcurrentHashMap<Long,Support> SUPPORT_REGISTRY = new ConcurrentHashMap<>();

        public static Support getSupport()
        {
            long id = COUNTER.incrementAndGet();
            Support support = new Support( id, SUPPORT_REGISTRY );
            SUPPORT_REGISTRY.put( id, support );
            return support;
        }

        public static Support getSupport( long supportId )
        {
            return SUPPORT_REGISTRY.get( supportId );
        }

        private final long id;
        private final ConcurrentHashMap<Long,Support> registry;
        public volatile DoubleLatch doubleLatch;
        public volatile DoubleLatch volatileLatch;
        public final List<Exception> exceptionsInProcedure;

        public Support( long id, ConcurrentHashMap<Long,Support> registry )
        {
            this.id = id;
            this.registry = registry;
            exceptionsInProcedure = Collections.synchronizedList( new ArrayList<>() );
        }

        public long getId()
        {
            return id;
        }

        @Override
        public void close()
        {
            registry.remove( id );
        }
    }

    @SuppressWarnings( {"unused", "WeakerAccess"} )
    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Transaction transaction;

        @Context
        public Log log;

        @Context
        public TerminationGuard guard;

        @Context
        public SecurityContext securityContext;

        @Procedure( name = "test.loop" )
        public void loop( @Name( "supportId" ) long supportId )
        {
            Support support = Support.getSupport( supportId );
            DoubleLatch latch = support.volatileLatch;

            if ( latch != null )
            {
                latch.startAndWaitForAllToStart();
            }
            try
            {
                //noinspection InfiniteLoopStatement
                while ( true )
                {
                    try
                    {
                        //noinspection BusyWait
                        Thread.sleep( 250 );
                    }
                    catch ( InterruptedException e )
                    {
                        //noinspection ResultOfMethodCallIgnored
                        Thread.interrupted();
                    }
                    guard.check();
                }
            }
            catch ( TransactionTerminatedException e )
            {
                if ( e.status().equals( TransactionTimedOut ) )
                {
                    throw new TransientTransactionFailureException( TransactionTimedOut, PROCEDURE_TIMEOUT_ERROR, e );
                }
                else
                {
                    throw e;
                }
            }
            finally
            {
                if ( latch != null )
                {
                    latch.finish();
                }
            }
        }

        @Procedure( name = "test.numNodes" )
        public Stream<CountResult> numNodes()
        {
            Long nNodes = transaction.getAllNodes().stream().count();
            return Stream.of( new CountResult( nNodes ) );
        }

        @Admin
        @SystemProcedure
        @Procedure( name = "test.adminReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> staticAdminReadProcedure()
        {
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.staticReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> staticReadProcedure()
        {
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.staticWriteProcedure", mode = Mode.WRITE )
        public Stream<AuthProceduresBase.StringResult> staticWriteProcedure()
        {
            transaction.createNode();
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.staticSchemaProcedure", mode = Mode.SCHEMA )
        public Stream<AuthProceduresBase.StringResult> staticSchemaProcedure()
        {
            if ( !securityContext.mode().allowsSchemaWrites() )
            {
                throw new AuthorizationViolationException( SCHEMA_OPS_NOT_ALLOWED );
            }
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.allowedReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> allowedProcedure1()
        {
            Result result = transaction.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "foo" ).toString() ) );
        }

        @Procedure( name = "test.otherAllowedReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> otherAllowedProcedure()
        {
            Result result = transaction.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "foo" ).toString() ) );
        }

        @Procedure( name = "test.allowedWriteProcedure", mode = Mode.WRITE )
        public Stream<AuthProceduresBase.StringResult> allowedProcedure2()
        {
            transaction.execute( "UNWIND [1, 2] AS i CREATE (:VeryUniqueLabel {prop: 'a'})" );
            Result result = transaction.execute( "MATCH (n:VeryUniqueLabel) RETURN n.prop AS a LIMIT 2" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "a" ).toString() ) );
        }

        @Procedure( name = "test.allowedSchemaProcedure", mode = Mode.SCHEMA )
        public Stream<AuthProceduresBase.StringResult> allowedProcedure3()
        {
            transaction.execute( "CREATE INDEX FOR (n:VeryUniqueLabel) ON (n.prop)" );
            transaction.execute( "DROP INDEX ON :VeryUniqueLabel(prop)" );
            return Stream.of( new AuthProceduresBase.StringResult( "OK" ) );
        }

        @Procedure( name = "test.nestedAllowedProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> nestedAllowedProcedure(
                @Name( "nestedProcedure" ) String nestedProcedure
        )
        {
            Result result = transaction.execute( "CALL " + nestedProcedure );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.doubleNestedAllowedProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> doubleNestedAllowedProcedure()
        {
            Result result = transaction
                    .execute( "CALL test.nestedAllowedProcedure('test.allowedReadProcedure') YIELD value" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.failingNestedAllowedWriteProcedure", mode = Mode.WRITE )
        public Stream<AuthProceduresBase.StringResult> failingNestedAllowedWriteProcedure()
        {
            Result result = transaction
                    .execute( "CALL test.nestedReadProcedure('test.allowedWriteProcedure') YIELD value" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.nestedReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> nestedReadProcedure(
                @Name( "nestedProcedure" ) String nestedProcedure
        )
        {
            Result result = transaction.execute( "CALL " + nestedProcedure );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.createNode", mode = WRITE )
        public void createNode()
        {
            transaction.createNode();
        }

        @Procedure( name = "test.threadTransaction", mode = WRITE )
        public void newThreadTransaction( @Name( "supportId" ) long supportId )
        {
            startWriteThread( supportId );
        }

        @Procedure( name = "test.threadReadDoingWriteTransaction" )
        public void threadReadDoingWriteTransaction( @Name( "supportId" ) long supportId )
        {
            startWriteThread( supportId );
        }

        private void startWriteThread( long supportId )
        {
            Support support = Support.getSupport( supportId );
            new Thread( () ->
            {
                support.doubleLatch.start();
                try ( Transaction tx = db.beginTx() )
                {
                    tx.createNode( Label.label( "VeryUniqueLabel" ) );
                    tx.commit();
                }
                catch ( Exception e )
                {
                    support.exceptionsInProcedure.add( e );
                }
                finally
                {
                    support.doubleLatch.finish();
                }
            } ).start();
        }
    }

    @SuppressWarnings( "unused" )
    public static class ClassWithFunctions
    {
        @Context
        public Transaction transaction;

        @UserFunction( name = "test.nonAllowedFunc" )
        public String nonAllowedFunc()
        {
            return "success";
        }

        @UserFunction( name = "test.allowedFunc" )
        public String allowedFunc()
        {
            return "success for role1";
        }

        @UserFunction( name = "test.allowedFunction1" )
        public String allowedFunction1()
        {
            Result result = transaction.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.next().get( "foo" ).toString();
        }

        @UserFunction( name = "test.allowedFunction2" )
        public String allowedFunction2()
        {
            Result result = transaction.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.next().get( "foo" ).toString();
        }

        @UserFunction( name = "test.nestedAllowedFunction" )
        public String nestedAllowedFunction(
                @Name( "nestedFunction" ) String nestedFunction
        )
        {
            Result result = transaction.execute( "RETURN " + nestedFunction + " AS value" );
            return result.next().get( "value" ).toString();
        }
    }

    protected abstract Object valueOf( Object obj );

    private BaseToObjectValueWriter<RuntimeException> writer()
    {
        return new BaseToObjectValueWriter<>()
        {
            @Override
            protected Node newNodeEntityById( long id )
            {
                return null;
            }

            @Override
            protected Relationship newRelationshipEntityById( long id )
            {
                return null;
            }

            @Override
            protected Point newPoint( CoordinateReferenceSystem crs, double[] coordinate )
            {
                return null;
            }
        };
    }
}
