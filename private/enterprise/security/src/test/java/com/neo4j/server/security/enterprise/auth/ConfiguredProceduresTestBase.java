/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.internal.util.collections.Sets.newSet;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

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
        configuredSetup( stringMap( GraphDatabaseSettings.transaction_timeout.name(), "4s" ) );

        assertFail( adminSubject, "CALL test.loop", PROCEDURE_TIMEOUT_ERROR );

        Result result = neo.getLocalGraph().execute(
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );

        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    void shouldSetAllowedToConfigSetting() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "nonEmpty" ) );
        GlobalProcedures globalProcedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class );

        ProcedureSignature numNodes = globalProcedures.procedure( new QualifiedName( new String[]{"test"}, "numNodes" ) ).signature();
        assertThat( Arrays.asList( numNodes.allowed() ), containsInAnyOrder( "nonEmpty" ) );
    }

    @Test
    void shouldSetAllowedToDefaultValueAndRunningWorks() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    void shouldRunProcedureWithMatchingWildcardAllowed() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    void shouldNotRunProcedureWithMismatchingWildCardAllowed() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "tes.*:role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        GlobalProcedures globalProcedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class );

        ProcedureSignature numNodes = globalProcedures.procedure( new QualifiedName( new String[]{"test"}, "numNodes" ) ).signature();
        assertThat( Arrays.asList( numNodes.allowed() ), empty() );
        assertFail( noneSubject, "CALL test.numNodes", "Read operations are not allowed" );
    }

    @Test
    void shouldNotSetProcedureAllowedIfSettingNotSet() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        GlobalProcedures globalProcedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class );

        ProcedureSignature numNodes = globalProcedures.procedure( new QualifiedName( new String[]{"test"}, "numNodes" ) ).signature();
        assertThat( Arrays.asList( numNodes.allowed() ), empty() );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @Test
    void shouldSetAllowedToConfigSettingForUDF() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "nonEmpty" ) );
        GlobalProcedures globalProcedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class );

        UserFunctionSignature funcSig = globalProcedures.function(
                new QualifiedName( new String[]{"test"}, "nonAllowedFunc" ) ).signature();
        assertThat( Arrays.asList( funcSig.allowed() ), containsInAnyOrder( "nonEmpty" ) );
    }

    @Test
    void shouldSetAllowedToDefaultValueAndRunningWorksForUDF() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        assertSuccess( neo.login( "noneSubject", "abc" ), "RETURN test.allowedFunc() AS c",
                itr -> assertKeyIs( itr, "c", "success for role1" ) );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @Test
    void shouldNotSetProcedureAllowedIfSettingNotSetForUDF() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        GlobalProcedures globalProcedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class );

        UserFunctionSignature funcSig = globalProcedures.function(
                new QualifiedName( new String[]{"test"}, "nonAllowedFunc" ) ).signature();
        assertThat( Arrays.asList( funcSig.allowed() ), empty() );
    }

    @Test
    void shouldSetWildcardRoleConfigOnlyIfNotAnnotated() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester" ) );

        userManager.newRole( "tester", "noneSubject" );

        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    void shouldSetAllMatchingWildcardRoleConfigs() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester;test.create*:other" ) );

        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        assertSuccess( readSubject, "CALL test.allowedReadProcedure", itr -> assertKeyIs( itr, "value", "foo" ) );
        assertSuccess( noneSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( readSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "5" ) );
    }

    @Test
    void shouldSetAllMatchingWildcardRoleConfigsWithDefaultForUDFs() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester;test.create*:other",
                SecuritySettings.default_allowed.name(), "default" ) );

        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "default", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        assertSuccess( noneSubject, "RETURN test.nonAllowedFunc() AS f", itr -> assertKeyIs( itr, "f", "success" ) );
        assertSuccess( readSubject, "RETURN test.allowedFunction1() AS f", itr -> assertKeyIs( itr, "f", "foo" ) );
        assertSuccess( readSubject, "RETURN test.nonAllowedFunc() AS f", itr -> assertKeyIs( itr, "f", "success" ) );
    }

    @Test
    void shouldHandleWriteAfterAllowedReadProcedureWithAuthDisabled() throws Throwable
    {
        neo = setUpNeoServer( stringMap( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

        neo.getLocalGraph().getDependencyResolver().resolveDependency( GlobalProcedures.class )
                .registerProcedure( ClassWithProcedures.class );

        S subject = neo.login( "no_auth", "" );
        assertEmpty( subject, "CALL test.allowedReadProcedure() YIELD value CREATE (:NewNode {name: value})" );
    }

    @Test
    void shouldHandleMultipleRolesSpecifiedForMapping() throws Throwable
    {
        // Given
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester, other" ) );

        // When
        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        // Then
        assertSuccess( readSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "4" ) );
    }

    @Test
    void shouldListCorrectRolesForDBMSProcedures() throws Throwable
    {
        configuredSetup( defaultConfiguration() );

        Map<String,Set<String>> expected = genericMap(
                "dbms.changePassword", newSet( READER, EDITOR, PUBLISHER, ARCHITECT, ADMIN ),
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
        configuredSetup( stringMap(
                SecuritySettings.procedure_roles.name(), "test.numNodes:counter,user",
                SecuritySettings.default_allowed.name(), "default" ) );

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
        configuredSetup( stringMap(
                SecuritySettings.procedure_roles.name(), "test.allowedFunc:counter,user",
                SecuritySettings.default_allowed.name(), "default" ) );

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
        configuredSetup( stringMap( GraphDatabaseSettings.kill_query_verbose.name(), "true" ) );

        String query = "CALL dbms.killQuery('query-9999999999')";
        Map<String,Object> expected = new HashMap<>();
        expected.put( "queryId", valueOf( "query-9999999999" ) );
        expected.put( "username", valueOf( "n/a" ) );
        expected.put( "message", valueOf( "No Query found with this id" ) );
        assertSuccess( adminSubject, query, r -> assertThat( r.next(), equalTo( expected ) ) );
    }

    @Test
    void shouldNotGiveNiceMessageAtFailWhenTryingToKillWhenConfigured() throws Throwable
    {
        configuredSetup( stringMap( GraphDatabaseSettings.kill_query_verbose.name(), "false" ) );
        String query = "CALL dbms.killQuery('query-9999999999')";
        assertSuccess( adminSubject, query, r ->

                assertThat( r.hasNext(), is( false ) ) );
    }

    @Test
    void shouldGiveNiceMessageAtFailWhenTryingToKillMoreThenOne() throws Throwable
    {
        //Given
        configuredSetup( stringMap( GraphDatabaseSettings.kill_query_verbose.name(), "true" ) );
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
