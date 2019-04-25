/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.InMemorySystemGraphOperations;

import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphInitializer;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphOperations;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

class CommercialLoginContextTest
{
    private static final LoginContext.PropertyKeyIdLookup token = prop -> -1;
    private static final String user = "user";

    private MultiRealmAuthManager authManager;
    private EnterpriseUserManager userManager;

    @BeforeEach
    void setup() throws Throwable
    {
        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations ops = new InMemorySystemGraphOperations( secureHasher );
        SystemGraphImportOptions importOptions =
                new SystemGraphImportOptions( false, true, true, false, InMemoryUserRepository::new, InMemoryRoleRepository::new, InMemoryUserRepository::new,
                        InMemoryRoleRepository::new, InMemoryUserRepository::new, InMemoryUserRepository::new );
        SystemGraphInitializer graphInitializer =
                new SystemGraphInitializer( mock( QueryExecutor.class ), ops, importOptions, secureHasher, mock( Log.class ) );
        SystemGraphRealm realm = new SystemGraphRealm( ops, graphInitializer, true, secureHasher, new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() ), true, true );
        authManager = new MultiRealmAuthManager( realm, Collections.singleton( realm ), new MemoryConstrainedCacheManager(),
                mock( SecurityLog.class ), false, false, Collections.emptyMap() );
        authManager.start();

        userManager = authManager.getUserManager();
        userManager.newUser( user, password( "password" ), false );
    }

    @Test
    void userWithAdminRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        userManager.addRoleToUser( PredefinedRoles.ADMIN, user );
        CommercialLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
        assertTrue( securityContext.isAdmin() );
    }

    @Test
    void userWithArchitectRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        userManager.addRoleToUser( PredefinedRoles.ARCHITECT, user );
        CommercialLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    void userWithPublisherRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        userManager.addRoleToUser( PredefinedRoles.PUBLISHER, user );
        CommercialLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads(), "should allow reads" );
        assertTrue( securityContext.mode().allowsWrites(), "should allow writes" );
        assertFalse( securityContext.mode().allowsSchemaWrites(), "should _not_ allow schema writes" );
    }

    @Test
    void userWithReaderRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        userManager.addRoleToUser( PredefinedRoles.READER, user );
        CommercialLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    void userWithNonPredefinedRoleShouldHaveNoPermissions() throws Throwable
    {
        // Given
        CommercialLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertFalse( securityContext.mode().allowsReads() );
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    void shouldHaveNoPermissionsAfterLogout() throws Throwable
    {
        // Given
        userManager.addRoleToUser( PredefinedRoles.ARCHITECT, user );
        CommercialLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );

        loginContext.subject().logout();

        securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );
        // Then
        assertFalse( securityContext.mode().allowsReads() );
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    private CommercialLoginContext login() throws InvalidAuthTokenException
    {
        return authManager.login( authToken( user, "password" ) );
    }
}
