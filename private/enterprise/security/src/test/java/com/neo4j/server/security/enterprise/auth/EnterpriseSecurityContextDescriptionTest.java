/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.configuration.SecuritySettings;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

class EnterpriseSecurityContextDescriptionTest
{
    private MultiRealmAuthManager authManager;
    private SystemGraphRealm realm;
    private PrincipalCollection principals;

    private final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;
    private final SimpleAuthorizationInfo testAuthInfo = new SimpleAuthorizationInfo( Set.of( PUBLISHER, "role1" ) );

    @BeforeEach
    void setUp() throws Throwable
    {
        realm = mock(SystemGraphRealm.class);
        when( realm.doGetAuthenticationInfo( any() ) ).thenReturn( new ShiroAuthenticationInfo( "mats", "SystemGraphRealm", AuthenticationResult.SUCCESS ) );
        when( realm.supports( any() ) ).thenReturn( true );

        authManager = new MultiRealmAuthManager( realm, Collections.singleton( realm ), new MemoryConstrainedCacheManager(), mock( SecurityLog.class ),
                                                 Config.defaults() );
        authManager.start();
        principals = new SimplePrincipalCollection( "mats", "SystemGraphRealm" );
    }

    @Test
    void shouldMakeNiceDescriptionWithoutAssignedRoles() throws Exception
    {
        // PUBLIC is always part of a users set of roles
        assertThat( context().description() ).isEqualTo( "user 'mats' with roles [PUBLIC]" );
    }

    @Test
    void shouldMakeNiceDescriptionWithRoles() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( testAuthInfo );
        assertThat( context().description() ).isEqualTo( "user 'mats' with roles [PUBLIC, publisher, role1]" );
    }

    @Test
    void shouldMakeNiceDescriptionWithMode() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( testAuthInfo );
        EnterpriseSecurityContext modified = context().withMode( AccessMode.Static.CREDENTIALS_EXPIRED );
        assertThat( modified.description() ).isEqualTo( "user 'mats' with CREDENTIALS_EXPIRED" );
    }

    @Test
    void shouldMakeNiceDescriptionRestricted() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( testAuthInfo );
        EnterpriseSecurityContext context = context();
        EnterpriseSecurityContext restricted =
                context.withMode( new RestrictedAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description() ).isEqualTo( "user 'mats' with roles [PUBLIC, publisher, role1] restricted to READ" );
    }

    @Test
    void shouldMakeNiceDescriptionOverridden() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( testAuthInfo );
        EnterpriseSecurityContext context = context();
        EnterpriseSecurityContext overridden =
                context.withMode( new OverriddenAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( overridden.description() ).isEqualTo( "user 'mats' with roles [PUBLIC, publisher, role1] overridden by READ" );
    }

    @Test
    void shouldMakeNiceDescriptionAuthDisabled()
    {
        EnterpriseSecurityContext disabled = EnterpriseSecurityContext.AUTH_DISABLED;
        assertThat( disabled.description() ).isEqualTo( "AUTH_DISABLED with FULL" );
    }

    @Test
    void shouldMakeNiceDescriptionAuthDisabledAndRestricted()
    {
        EnterpriseSecurityContext disabled = EnterpriseSecurityContext.AUTH_DISABLED;
        EnterpriseSecurityContext restricted =
                disabled.withMode( new RestrictedAccessMode( disabled.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description() ).isEqualTo( "AUTH_DISABLED with FULL restricted to READ" );
    }

    private EnterpriseSecurityContext context() throws InvalidAuthTokenException
    {
        return authManager.login( authToken( "mats", "foo" ) )
                .authorize( token, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
    }
}
