/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class EnterpriseSecurityContextDescriptionTest
{
    private MultiRealmAuthManager authManager;
    private SystemGraphRealm realm;
    private PrincipalCollection principals;

    private final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;

    @Before
    public void setUp() throws Throwable
    {
        realm = mock(SystemGraphRealm.class);
        when( realm.doGetAuthenticationInfo( any() ) ).thenReturn( new ShiroAuthenticationInfo( "mats", "SystemGraphRealm", AuthenticationResult.SUCCESS ) );
        when( realm.supports( any() ) ).thenReturn( true );
        authManager = new MultiRealmAuthManager( realm, Collections.singleton( realm ), new MemoryConstrainedCacheManager(), mock( SecurityLog.class ), false );
        authManager.start();
        principals = new SimplePrincipalCollection( "mats", "SystemGraphRealm" );
    }

    @Test
    public void shouldMakeNiceDescriptionWithoutRoles() throws Exception
    {
        assertThat( context().description(), equalTo( "user 'mats' with no roles" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithRoles() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PUBLISHER, "role1" ) ) );
        assertThat( context().description(), equalTo( "user 'mats' with roles [publisher,role1]" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithMode() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PUBLISHER, "role1" ) ) );
        EnterpriseSecurityContext modified = context().withMode( AccessMode.Static.CREDENTIALS_EXPIRED );
        assertThat( modified.description(), equalTo( "user 'mats' with CREDENTIALS_EXPIRED" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionRestricted() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PUBLISHER, "role1" ) ) );
        EnterpriseSecurityContext context = context();
        EnterpriseSecurityContext restricted =
                context.withMode( new RestrictedAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "user 'mats' with roles [publisher,role1] restricted to READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionOverridden() throws Exception
    {
        when( realm.getAuthorizationInfoSnapshot( principals ) ).thenReturn( new SimpleAuthorizationInfo( Set.of( PUBLISHER, "role1" ) ) );
        EnterpriseSecurityContext context = context();
        EnterpriseSecurityContext overridden =
                context.withMode( new OverriddenAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( overridden.description(), equalTo( "user 'mats' with roles [publisher,role1] overridden by READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabled()
    {
        EnterpriseSecurityContext disabled = EnterpriseSecurityContext.AUTH_DISABLED;
        assertThat( disabled.description(), equalTo( "AUTH_DISABLED with FULL" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabledAndRestricted()
    {
        EnterpriseSecurityContext disabled = EnterpriseSecurityContext.AUTH_DISABLED;
        EnterpriseSecurityContext restricted =
                disabled.withMode( new RestrictedAccessMode( disabled.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "AUTH_DISABLED with FULL restricted to READ" ) );
    }

    private EnterpriseSecurityContext context() throws InvalidAuthTokenException, KernelException
    {
        return authManager.login( authToken( "mats", "foo" ) )
                .authorize( token, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
    }
}
