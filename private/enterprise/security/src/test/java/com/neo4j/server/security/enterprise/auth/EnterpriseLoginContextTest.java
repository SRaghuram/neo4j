/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LabelSegment;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.RelTypeSegment;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.database.TestDefaultDatabaseResolver;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

class EnterpriseLoginContextTest
{
    private static final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;

    private MultiRealmAuthManager authManager;
    private SystemGraphRealm realm;

    private ResourcePrivilege accessPrivilege;
    private ResourcePrivilege matchNodePrivilege;
    private ResourcePrivilege matchRelPrivilege;
    private ResourcePrivilege writeNodePrivilege;
    private ResourcePrivilege writeRelPrivilege;
    private ResourcePrivilege tokenPrivilege;
    private ResourcePrivilege indexPrivilege;
    private ResourcePrivilege constraintPrivilege;
    private ResourcePrivilege adminPrivilege;

    @BeforeEach
    void setup() throws Throwable
    {
        realm = mock( SystemGraphRealm.class );
        when( realm.doGetAuthenticationInfo( any() ) ).thenReturn( new ShiroAuthenticationInfo( "user", "SystemGraphRealm", AuthenticationResult.SUCCESS ) );
        when( realm.supports( any() ) ).thenReturn( true );
        createPrivileges();
        var privResolver = new PrivilegeResolver( realm, Config.defaults() );
        authManager = new MultiRealmAuthManager( privResolver, Collections.singleton( realm ), new MemoryConstrainedCacheManager(), mock( SecurityLog.class ),
                                                 Config.defaults(), new TestDefaultDatabaseResolver( DEFAULT_DATABASE_NAME ) );
        authManager.start();

    }

    private void createPrivileges() throws InvalidArgumentsException
    {
        accessPrivilege = new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        matchNodePrivilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, SpecialDatabase.ALL );
        matchRelPrivilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, SpecialDatabase.ALL );
        writeNodePrivilege = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, SpecialDatabase.ALL );
        writeRelPrivilege = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), RelTypeSegment.ALL, SpecialDatabase.ALL );
        tokenPrivilege = new ResourcePrivilege( GRANT, TOKEN, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        indexPrivilege = new ResourcePrivilege( GRANT, INDEX, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        constraintPrivilege = new ResourcePrivilege( GRANT, CONSTRAINT, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        adminPrivilege = new ResourcePrivilege( GRANT, ADMIN, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
    }

    @Test
    void userWithAdminRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PredefinedRoles.ADMIN ) ) );
        when( realm.getPrivilegesForRoles( Set.of( PredefinedRoles.PUBLIC, PredefinedRoles.ADMIN ) ) ).thenReturn( new HashSet<>(
                Arrays.asList( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege, tokenPrivilege, indexPrivilege,
                        constraintPrivilege, adminPrivilege ) ) );

        var adminToken = mock( LoginContext.IdLookup.class );
        when( adminToken.getAdminProcedureIds() ).thenReturn( new int[]{42} );

        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( adminToken, DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
        assertTrue( securityContext.allowExecuteAdminProcedure( 42 ) );
    }

    @Test
    void userWithArchitectRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PredefinedRoles.ARCHITECT ) ) );
        when( realm.getPrivilegesForRoles( Set.of( PredefinedRoles.PUBLIC, PredefinedRoles.ARCHITECT ) ) ).thenReturn( new HashSet<>(
                Arrays.asList( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege, tokenPrivilege, indexPrivilege,
                        constraintPrivilege ) ) );
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
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PredefinedRoles.PUBLISHER ) ) );
        when( realm.getPrivilegesForRoles( Set.of( PredefinedRoles.PUBLIC, PredefinedRoles.PUBLISHER ) ) ).thenReturn( new HashSet<>(
                Arrays.asList( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege, tokenPrivilege ) ) );

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
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PredefinedRoles.READER ) ) );
        when( realm.getPrivilegesForRoles( Set.of( PredefinedRoles.PUBLIC, PredefinedRoles.READER ) ) ).thenReturn( new HashSet<>(
                Arrays.asList( accessPrivilege, matchNodePrivilege, matchRelPrivilege ) ) );
        EnterpriseLoginContext loginContext = login();

        // When
        SecurityContext securityContext = loginContext.authorize( token, DEFAULT_DATABASE_NAME );

        // Then
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    void usersShouldHavePublicRole() throws Throwable
    {
        // Given
        when( realm.getAuthorizationInfoSnapshot( any() ) ).thenReturn( new SimpleAuthorizationInfo( Collections.emptySet() ) );
        EnterpriseLoginContext loginContext = login();

        assertThat( loginContext.roles() ).isEqualTo( Set.of( PredefinedRoles.PUBLIC ) );
        // When
    }

    @Test
    void userWithNonPredefinedRoleShouldHaveNoPermissions() throws Throwable
    {
        // Given
        EnterpriseLoginContext loginContext = login();

        // When
        assertThrows( AuthorizationViolationException.class, () -> loginContext.authorize( token, DEFAULT_DATABASE_NAME ) );
    }

    private EnterpriseLoginContext login() throws InvalidAuthTokenException
    {
        return authManager.login( authToken( "user", "password" ) );
    }
}
