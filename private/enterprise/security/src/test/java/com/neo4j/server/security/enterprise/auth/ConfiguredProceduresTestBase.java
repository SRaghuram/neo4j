/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.internal.util.collections.Sets.newSet;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;

public abstract class ConfiguredProceduresTestBase<S> extends ProcedureInteractionTestBase<S>
{

    @Override
    public void setUp()
    {
        // tests are required to setup database with specific configs
    }

    @Test
    void shouldTerminateLongRunningProcedureThatChecksTheGuardRegularlyOnTimeout() throws Throwable
    {
        configuredSetup( Map.of( GraphDatabaseSettings.procedure_roles, "test.*:tester" ) );

        createRoleWithAccess( "tester", "noneSubject" );

        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    void shouldSetAllMatchingWildcardRoleConfigs() throws Throwable
    {
        configuredSetup( Map.of( GraphDatabaseSettings.procedure_roles, "test.*:tester;test.create*:other" ) );

        createRoleWithAccess( "tester", "noneSubject" );
        createRoleWithAccess( "other", "readSubject" );

        assertSuccess( readSubject, "CALL test.allowedReadProcedure", itr -> assertKeyIs( itr, "value", "foo" ) );
        assertSuccess( noneSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( readSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "5" ) );
    }

    @Test
    void shouldSetAllMatchingWildcardRoleConfigsWithDefaultForUDFs() throws Throwable
    {
        configuredSetup( Map.of( GraphDatabaseSettings.procedure_roles, "test.*:tester;test.create*:other",
                GraphDatabaseSettings.default_allowed, "default" ) );

        createRoleWithAccess( "tester", "noneSubject" );
        createRoleWithAccess( "default", "noneSubject" );
        createRoleWithAccess( "other", "readSubject" );

        assertSuccess( noneSubject, "RETURN test.nonAllowedFunc() AS f", itr -> assertKeyIs( itr, "f", "success" ) );
        assertSuccess( readSubject, "RETURN test.allowedFunction1() AS f", itr -> assertKeyIs( itr, "f", "foo" ) );
        assertSuccess( readSubject, "RETURN test.nonAllowedFunc() AS f", itr -> assertKeyIs( itr, "f", "success" ) );
    }

    @Test
    void shouldHandleWriteAfterAllowedReadProcedureWithAuthDisabled() throws Throwable
    {
        neo = setUpNeoServer( Map.of( GraphDatabaseSettings.auth_enabled, FALSE ) );

        neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class )
                .registerProcedure( ClassWithProcedures.class );

        S subject = neo.login( "no_auth", "" );
        assertEmpty( subject, "CALL test.allowedReadProcedure() YIELD value CREATE (:NewNode {name: value})" );
    }

    @Test
    void shouldHandleMultipleRolesSpecifiedForMapping() throws Throwable
    {
        // Given
        configuredSetup( Map.of( GraphDatabaseSettings.procedure_roles, "test.*:tester, other" ) );

        // When
        createRoleWithAccess( "tester", "noneSubject" );
        createRoleWithAccess( "other", "readSubject" );

        // Then
        assertSuccess( readSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "4" ) );
    }

    @Test
    void shouldListCorrectRolesForDBMSProcedures() throws Throwable
    {
        configuredSetup( defaultConfiguration() );

        Map<String,Set<String>> expected = genericMap(
                "dbms.functions", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.killQueries", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.killQuery", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.listActiveLocks", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.listConfig", newSet( ADMIN ),
                "dbms.listQueries", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.procedures", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.security.activateUser", newSet( ADMIN ),
                "dbms.security.addRoleToUser", newSet( ADMIN ),
                "dbms.security.changePassword", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.security.changeUserPassword", newSet( ADMIN ),
                "dbms.security.clearAuthCache", newSet( ADMIN ),
                "dbms.security.createRole", newSet( ADMIN ),
                "dbms.security.createUser", newSet( ADMIN ),
                "dbms.security.deleteRole", newSet( ADMIN ),
                "dbms.security.deleteUser", newSet( ADMIN ),
                "dbms.security.listRoles", newSet( ADMIN ),
                "dbms.security.listRolesForUser", newSet( ADMIN ),
                "dbms.security.listUsers", newSet( ADMIN ),
                "dbms.security.listUsersForRole", newSet( ADMIN ),
                "dbms.security.removeRoleFromUser", newSet( ADMIN ),
                "dbms.security.showCurrentUser", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.showCurrentUser", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.security.suspendUser", newSet( ADMIN ),
                "dbms.setTXMetaData", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ) );

        assertListProceduresHasRoles( readSubject, expected, "CALL dbms.procedures" );
    }

    @Test
    void shouldShowAllowedRolesWhenListingProcedures() throws Throwable
    {
        configuredSetup( Map.of(
                GraphDatabaseSettings.procedure_roles, "test.numNodes:counter,user",
                GraphDatabaseSettings.default_allowed, "default" ) );

        Map<String,Set<String>> expected = genericMap(
                "test.staticReadProcedure", newSet( "default", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "test.staticWriteProcedure", newSet( "default", EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "test.staticSchemaProcedure", newSet( "default", ARCHITECT, ADMIN ),
                "test.annotatedProcedure", newSet( "annotated", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "test.numNodes", newSet( "counter", "user", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "db.labels", newSet( "default", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.security.changePassword", newSet( "default", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.procedures", newSet( "default", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.listQueries", newSet( "default", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.security.createUser", newSet( ADMIN ),
                "db.createLabel", newSet( "default", EDITOR, PUBLISHER, ARCHITECT, ADMIN ) );

        String call = "CALL dbms.procedures";
        assertListProceduresHasRoles( adminSubject, expected, call );
        assertListProceduresHasRoles( schemaSubject, expected, call );
        assertListProceduresHasRoles( writeSubject, expected, call );
        assertListProceduresHasRoles( readSubject, expected, call );
    }

    @Test
    void shouldShowAllowedRolesWhenListingFunctions() throws Throwable
    {
        configuredSetup( Map.of(
                GraphDatabaseSettings.procedure_roles, "test.allowedFunc:counter,user",
                GraphDatabaseSettings.default_allowed, "default" ) );

        Map<String,Set<String>> expected = genericMap(
                "test.annotatedFunction", newSet( "annotated", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "test.allowedFunc", newSet( "counter", "user", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
                "test.nonAllowedFunc", newSet( "default", READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ) );

        String call = "CALL dbms.functions";
        assertListProceduresHasRoles( adminSubject, expected, call );
        assertListProceduresHasRoles( schemaSubject, expected, call );
        assertListProceduresHasRoles( writeSubject, expected, call );
        assertListProceduresHasRoles( readSubject, expected, call );
    }

    @Test
    void shouldGiveNiceMessageAtFailWhenTryingToKill() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        String query = "CALL dbms.killQuery('query-9999999999')";
        Map<String,Object> expected = new HashMap<>();
        expected.put( "queryId", valueOf( "query-9999999999" ) );
        expected.put( "username", valueOf( "n/a" ) );
        expected.put( "message", valueOf( "No Query found with this id" ) );
        assertSuccess( adminSubject, query, r -> assertThat( r.next(), equalTo( expected ) ) );
    }

    @Test
    void shouldGiveNiceMessageAtFailWhenTryingToKillMoreThenOne() throws Throwable
    {
        //Given
        configuredSetup( defaultConfiguration() );
        String query = "CALL dbms.killQueries(['query-9999999999', 'query-9999999989'])";

        //Expect
        Set<Map<String,Object>> expected = new HashSet<>();
        Map<String,Object> firstResultExpected = new HashMap<>();
        firstResultExpected.put( "queryId", valueOf( "query-9999999989" ) );
        firstResultExpected.put( "username", valueOf( "n/a" ) );
        firstResultExpected.put( "message", valueOf( "No Query found with this id" ) );
        Map<String,Object> secondResultExpected = new HashMap<>();
        secondResultExpected.put( "queryId", valueOf( "query-9999999999" ) );
        secondResultExpected.put( "username", valueOf( "n/a" ) );
        secondResultExpected.put( "message", valueOf( "No Query found with this id" ) );
        expected.add( firstResultExpected );
        expected.add( secondResultExpected );

        //Then
        assertSuccess( adminSubject, query, r ->
        {
            Set<Map<String,Object>> actual = r.stream().collect( toSet() );
            assertThat( actual, equalTo( expected ) );
        } );
    }

    private void assertListProceduresHasRoles( S subject, Map<String,Set<String>> expected, String call )
    {
        assertSuccess( subject, call, itr ->
        {
            List<String> failures = itr.stream().filter( record ->
            {
                String name = toRawValue( record.get( "name" ) ).toString();
                List<?> roles = (List<?>) toRawValue( record.get( "roles" ) );
                return expected.containsKey( name ) && !expected.get( name ).equals( new HashSet<>( roles ) );
            } ).map( record ->
            {
                String name = toRawValue( record.get( "name" ) ).toString();
                return name + ": expected '" + expected.get( name ) + "' but was '" + record.get( "roles" ) + "'";
            } ).collect( toList() );

            assertThat( "Expectations violated: " + failures.toString(), failures.isEmpty() );
        } );
    }
}
