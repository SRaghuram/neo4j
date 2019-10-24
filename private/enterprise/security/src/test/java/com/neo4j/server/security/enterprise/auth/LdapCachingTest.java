/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.google.common.testing.FakeTicker;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.subject.PrincipalCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.naming.NamingException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

class LdapCachingTest
{
    private MultiRealmAuthManager authManager;
    private TestRealm testRealm;
    private FakeTicker fakeTicker;

    private final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;

    @BeforeEach
    void setup() throws Throwable
    {
        SecurityLog securityLog = mock( SecurityLog.class );

        testRealm = new TestRealm( getLdapConfig(), securityLog, new SecureHasher() );

        fakeTicker = new FakeTicker();
        EnterpriseUserManager userManager = mock( EnterpriseUserManager.class );
        when( userManager.getPrivilegesForRoles( anySet() ) ).thenReturn( Collections.singleton(
                new ResourcePrivilege( ResourcePrivilege.GrantOrDeny.GRANT, PrivilegeAction.ACCESS, new Resource.DatabaseResource(), DatabaseSegment.ALL ) ) );

        authManager = new MultiRealmAuthManager( userManager, Collections.singletonList( testRealm ),
                                                 new ShiroCaffeineCache.Manager( fakeTicker::read, 100, 10, true ),
                                                 securityLog, false );
        authManager.init();
        authManager.start();
    }

    private static Config getLdapConfig()
    {
        return Config.newBuilder()
                .set( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" )
                .set( SecuritySettings.ldap_authorization_group_membership_attribute_names, List.of( "gidnumber" ) )
                .set( SecuritySettings.ldap_authorization_use_system_account, true )
                .build();
    }

    @Test
    void shouldCacheAuthenticationInfo() throws InvalidAuthTokenException
    {
        // Given
        authManager.login( authToken( "mike", "123" ) );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        authManager.login( authToken( "mike", "123" ) );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthenticationFlag(), is( false ) );
    }

    @Test
    void shouldCacheAuthorizationInfo() throws Exception
    {
        // Given
        EnterpriseLoginContext mike = authManager.login( authToken( "mike", "123" ) );
        mike.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        mike.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthorizationFlag(), is( false ) );
    }

    @Test
    void shouldInvalidateAuthorizationCacheAfterTTL() throws Exception
    {
        // Given
        EnterpriseLoginContext mike = authManager.login( authToken( "mike", "123" ) );
        mike.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        fakeTicker.advance( 99, TimeUnit.MILLISECONDS );
        mike.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthorizationFlag(), is( false ) );

        // When
        fakeTicker.advance( 2, TimeUnit.MILLISECONDS );
        mike.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertThat( "Test realm did not received a call", testRealm.takeAuthorizationFlag(), is( true ) );
    }

    @Test
    void shouldInvalidateAuthenticationCacheAfterTTL() throws InvalidAuthTokenException
    {
        // Given
        Map<String,Object> mike = authToken( "mike", "123" );
        authManager.login( mike );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        fakeTicker.advance( 99, TimeUnit.MILLISECONDS );
        authManager.login( mike );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthenticationFlag(), is( false ) );

        // When
        fakeTicker.advance( 2, TimeUnit.MILLISECONDS );
        authManager.login( mike );

        // Then
        assertThat( "Test realm did not received a call", testRealm.takeAuthenticationFlag(), is( true ) );
    }

    @Test
    void shouldInvalidateAuthenticationCacheOnDemand() throws InvalidAuthTokenException
    {
        // Given
        Map<String,Object> mike = authToken( "mike", "123" );
        authManager.login( mike );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        fakeTicker.advance( 2, TimeUnit.MILLISECONDS );
        authManager.login( mike );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthenticationFlag(), is( false ) );

        // When
        authManager.clearAuthCache();
        authManager.login( mike );

        // Then
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );
    }

    private class TestRealm extends LdapRealm
    {
        private boolean authenticationFlag;
        private boolean authorizationFlag;

        boolean takeAuthenticationFlag()
        {
            boolean t = authenticationFlag;
            authenticationFlag = false;
            return t;
        }

        boolean takeAuthorizationFlag()
        {
            boolean t = authorizationFlag;
            authorizationFlag = false;
            return t;
        }

        TestRealm( Config config, SecurityLog securityLog, SecureHasher secureHasher )
        {
            super( config, securityLog, secureHasher, true, true );
            setAuthenticationCachingEnabled( true );
            setAuthorizationCachingEnabled( true );
        }

        @Override
        public String getName()
        {
            return "TestRealm wrapping " + super.getName();
        }

        @Override
        public boolean supports( AuthenticationToken token )
        {
            return super.supports( token );
        }

        @Override
        protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
        {
            authenticationFlag = true;
            try
            {
                return createAuthenticationInfo( token, token.getPrincipal(), token.getCredentials(), null );
            }
            catch ( NamingException e )
            {
                throw new AuthenticationException( e.getMessage() );
            }
        }

        @Override
        protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
        {
            authorizationFlag = true;
            return new AuthorizationInfo()
            {
                @Override
                public Collection<String> getRoles()
                {
                    return Collections.emptyList();
                }

                @Override
                public Collection<String> getStringPermissions()
                {
                    return Collections.emptyList();
                }

                @Override
                public Collection<Permission> getObjectPermissions()
                {
                    return Collections.emptyList();
                }
            };
        }
    }

}
