/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SCHEMA;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

class EnterpriseLoginContextTest
{
    private static final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;

    private MultiRealmAuthManager authManager;
    private SystemGraphRealm realm;

    private ResourcePrivilege accessPrivilege;
    private ResourcePrivilege traverseNodePrivilege;
    private ResourcePrivilege traverseRelPrivilege;
    private ResourcePrivilege readNodePrivilege;
    private ResourcePrivilege readRelPrivilege;
    private ResourcePrivilege writeNodePrivilege;
    private ResourcePrivilege writeRelPrivilege;
    private ResourcePrivilege tokenPrivilege;
    private ResourcePrivilege schemaPrivilege;
    private ResourcePrivilege adminPrivilege;

    @BeforeEach
    void setup() throws Throwable
    {
        realm = mock( SystemGraphRealm.class );
        when( realm.doGetAuthenticationInfo( any() ) ).thenReturn( new ShiroAuthenticationInfo( "user", "SystemGraphRealm", AuthenticationResult.SUCCESS ) );
        when( realm.supports( any() ) ).thenReturn( true );
        createPrivileges();

        authManager = new MultiRealmAuthManager( realm, Collections.singleton( realm ), new MemoryConstrainedCacheManager(), mock( SecurityLog.class ), false );
        authManager.start();

    }

    private void createPrivileges() throws InvalidArgumentsException
    {
        accessPrivilege = new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), DatabaseSegment.ALL );
        traverseNodePrivilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL );
        traverseRelPrivilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL );
        readNodePrivilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL );
        readRelPrivilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL );
        writeNodePrivilege = new ResourcePrivilege( GRANT, WRITE, new Resource.AllPropertiesResource(), LabelSegment.ALL );
        writeRelPrivilege = new ResourcePrivilege( GRANT, WRITE, new Resource.AllPropertiesResource(), RelTypeSegment.ALL );
        tokenPrivilege = new ResourcePrivilege( GRANT, TOKEN, new Resource.DatabaseResource(), DatabaseSegment.ALL );
        schemaPrivilege = new ResourcePrivilege( GRANT, SCHEMA, new Resource.DatabaseResource(), DatabaseSegment.ALL );
        adminPrivilege = new ResourcePrivilege( GRANT, ADMIN, new Resource.DatabaseResource(), DatabaseSegment.ALL );
    }

    @Test
    void userWithAdminRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Collections.singleton( PredefinedRoles.ADMIN ) ) );
        when( realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ADMIN ) ) ).thenReturn(
                Set.of( accessPrivilege, traverseNodePrivilege, traverseRelPrivilege, readNodePrivilege, readRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenPrivilege, schemaPrivilege, adminPrivilege ) );
        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
        assertTrue( securityContext.isAdmin() );
    }

    @Test
    void userWithArchitectRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Collections.singleton( PredefinedRoles.ARCHITECT ) ) );
        when( realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ARCHITECT ) ) ).thenReturn(
                Set.of( accessPrivilege, traverseNodePrivilege, traverseRelPrivilege, readNodePrivilege, readRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenPrivilege, schemaPrivilege ) );
        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    void userWithPublisherRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Collections.singleton( PredefinedRoles.PUBLISHER ) ) );
        when( realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.PUBLISHER ) ) ).thenReturn(
                Set.of( accessPrivilege, traverseNodePrivilege, traverseRelPrivilege, readNodePrivilege, readRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenPrivilege ) );
        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsWrites(), "should allow writes" );
        assertFalse( securityContext.mode().allowsSchemaWrites(), "should _not_ allow schema writes" );
    }

    @Test
    void userWithReaderRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Collections.singleton( PredefinedRoles.READER ) ) );
        when( realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.READER ) ) ).thenReturn(
                Set.of( accessPrivilege, traverseNodePrivilege, traverseRelPrivilege, readNodePrivilege, readRelPrivilege ) );
        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    void userWithNonPredefinedRoleShouldHaveNoPermissions() throws Throwable
    {
        // Given
        EnterpriseLoginContext loginContext = login();

        // When
        assertThrows( AuthorizationViolationException.class, () -> loginContext.authorize( token, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldHaveNoPermissionsAfterLogout() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Collections.singleton( PredefinedRoles.ARCHITECT ) ) );
        when( realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ARCHITECT ) ) ).thenReturn(
                Set.of( accessPrivilege, traverseNodePrivilege, traverseRelPrivilege, readNodePrivilege, readRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenPrivilege, schemaPrivilege ) );
        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );

        loginContext.subject().logout();

        // Then
        assertThrows( AuthorizationViolationException.class, () -> loginContext.authorize( token, DEFAULT_DATABASE_NAME ) );
    }

    private EnterpriseLoginContext login() throws InvalidAuthTokenException
    {
        return authManager.login( authToken( "user", "password" ) );
    }
}
