/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.InMemoryUserManager;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

class EnterpriseLoginContextTest
{
    private static final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;
    private static final String user = "user";

    private MultiRealmAuthManager authManager;
    private InMemoryUserManager userManager;

    @BeforeEach
    void setup() throws Throwable
    {
        userManager = new InMemoryUserManager( Config.defaults() );
        authManager =
                new MultiRealmAuthManager( userManager, Collections.singleton( userManager ), new MemoryConstrainedCacheManager(), mock( SecurityLog.class ), false );
        authManager.start();
        userManager.newUser( user, password( "password" ), false );
    }

    @Test
    void userWithAdminRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        userManager.addRoleToUser( PredefinedRoles.ADMIN, user );
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
        userManager.addRoleToUser( PredefinedRoles.ARCHITECT, user );
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
        userManager.addRoleToUser( PredefinedRoles.PUBLISHER, user );
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
        userManager.addRoleToUser( PredefinedRoles.READER, user );
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
        userManager.addRoleToUser( PredefinedRoles.ARCHITECT, user );
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
        return authManager.login( authToken( user, "password" ) );
    }
}
