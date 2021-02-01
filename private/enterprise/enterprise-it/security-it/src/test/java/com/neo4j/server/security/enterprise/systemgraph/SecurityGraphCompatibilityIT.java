/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class SecurityGraphCompatibilityIT extends SecurityGraphCompatibilityTestBase
{
    private static final EnterpriseSecurityGraphComponentVersion[] SUPPORTED_PREVIOUS_VERSIONS =
            Arrays.stream( EnterpriseSecurityGraphComponentVersion.values() )
                  .filter( version -> version.runtimeSupported() &&
                                      version.getVersion() < EnterpriseSecurityGraphComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION )
                  .toArray( EnterpriseSecurityGraphComponentVersion[]::new );

    @ParameterizedTest
    @MethodSource( "supportedPreviousVersions" )
    void shouldAuthenticate( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        initEnterprise( version );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldNotAuthorizeOn36() throws Exception
    {
        initEnterprise( EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_36 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.SUCCESS );

        // Access to System is allowed but with no other privileges
        SecurityContext securityContextSystem = loginContext.authorize( LoginContext.IdLookup.EMPTY, SYSTEM_DATABASE_NAME );
        var systemMode = securityContextSystem.mode();
        assertThat( systemMode.allowsReadPropertyAllLabels( -1 ) ).isFalse();
        assertThat( systemMode.allowsTraverseAllLabels() ).isFalse();
        assertThat( systemMode.allowsWrites() ).isFalse();

        // Access to neo4j is disallowed
        assertThrows( AuthorizationViolationException.class, () -> loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @ParameterizedTest
    @MethodSource( "supportedPreviousVersions" )
    void shouldAuthorize( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        initEnterprise( version );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        SecurityContext securityContext = loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME );
        assertThat( securityContext.mode().allowsReadPropertyAllLabels( -1 ) ).isTrue();
        assertThat( securityContext.mode().allowsTraverseAllLabels() ).isTrue();
        assertThat( securityContext.mode().allowsWrites() ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isTrue();
    }

    @ParameterizedTest
    @MethodSource( "supportedPreviousVersions" )
    void shouldMakeSchemaChanges( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        initEnterprise( version );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();
        GraphDatabaseAPI graph = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = graph.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "CREATE INDEX ON :User(name)" );
            result.accept( r -> true );
            result.close();
            tx.commit();
            assertThat( result.getQueryStatistics().getIndexesAdded() ).isEqualTo( 1 );
        }
        try ( Transaction tx = graph.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            tx.execute( "CALL db.awaitIndexes()" ).accept( r -> true );
            tx.commit();
        }
        try ( Transaction tx = graph.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "CALL db.indexes()" );
            List<String> indexes = result.stream().map( this::asCypherIndex ).collect( Collectors.toList() );
            result.close();
            MatcherAssert.assertThat( "Expected to find index", indexes, hasItem( ":User(name)" ) );
            tx.commit();
        }
    }

    private String asCypherIndex( Map<String,Object> row )
    {
        Function<String,String> asString = key -> ((ArrayList<?>) row.get( key )).stream().map( Object::toString ).collect( Collectors.joining( ", " ) );
        return String.format( ":%s(%s)", asString.apply( "labelsOrTypes" ), asString.apply( "properties" ) );
    }

    @ParameterizedTest
    @MethodSource( "versionsAndShowCommands" )
    void showPrivilegesShouldSucceedOnOldGraph( EnterpriseSecurityGraphComponentVersion version, String query, String schemaAction ) throws Exception
    {
        initEnterprise( version );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();
        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( query );
            List<String> schemaRoles =
                    result.stream().filter( row -> row.get( "action" ).equals( schemaAction ) ).map( row -> (String) row.get( "role" ) ).collect(
                            Collectors.toList() );
            MatcherAssert.assertThat( "Expect admin to have schema-edit capabilities", schemaRoles, hasItem( "admin" ) );
        }
    }

    @ParameterizedTest
    @MethodSource( "supportedPreviousVersions" )
    void shouldHaveExecutePrivilegeByDefault( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        initEnterprise( version );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();
        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, loginContext ) )
        {
            Result result = tx.execute( "CALL db.labels()" );
            assertThat( result.hasNext() ).isFalse();
        }
    }

    private static Stream<Arguments> versionsAndShowCommands()
    {
        Function<EnterpriseSecurityGraphComponentVersion,String> actFor =
                version -> version.equals( EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_40 ) ? "schema" : "index";
        String[] queries = {"SHOW PRIVILEGES", "SHOW USER neo4j PRIVILEGES", "SHOW ROLE admin PRIVILEGES"};
        return Arrays.stream( SUPPORTED_PREVIOUS_VERSIONS )
                     .flatMap( version -> Arrays.stream( queries ).map( query -> Arguments.of( version, query, actFor.apply( version ) ) ) );
    }

    private static Stream<Arguments> supportedPreviousVersions()
    {
        return Arrays.stream( SUPPORTED_PREVIOUS_VERSIONS ).map( Arguments::of );
    }
}
