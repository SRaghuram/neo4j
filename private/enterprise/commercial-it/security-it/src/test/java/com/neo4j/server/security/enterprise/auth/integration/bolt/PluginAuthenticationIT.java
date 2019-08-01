/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthenticationPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.TestCustomCacheableAuthenticationPlugin;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.server.security.enterprise.configuration.SecuritySettings.PLUGIN_REALM_NAME_PREFIX;
import static com.neo4j.server.security.enterprise.configuration.SecuritySettings.authentication_providers;
import static com.neo4j.server.security.enterprise.configuration.SecuritySettings.authorization_providers;
import static org.apache.commons.lang3.StringUtils.prependIfMissing;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class PluginAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    private static final List<String> defaultTestPluginRealmList = Arrays.asList(
            "TestAuthenticationPlugin",
            "TestAuthPlugin",
            "TestCacheableAdminAuthPlugin",
            "TestCacheableAuthenticationPlugin",
            "TestCacheableAuthPlugin",
            "TestCustomCacheableAuthenticationPlugin",
            "TestCustomParametersAuthenticationPlugin"
    );

    private static final List<String> DEFAULT_TEST_PLUGIN_REALMS = defaultTestPluginRealmList.stream()
            .map( s -> prependIfMissing( s, PLUGIN_REALM_NAME_PREFIX ) )
            .collect( Collectors.toList() );

    @Override
    protected Map<Setting<?>,Object> getSettings()
    {
        return Map.of( authentication_providers, DEFAULT_TEST_PLUGIN_REALMS, authorization_providers, DEFAULT_TEST_PLUGIN_REALMS ); }

    @Test
    public void shouldAuthenticateWithTestAuthenticationPlugin()
    {
        assertAuth( "neo4j", "neo4j", "plugin-TestAuthenticationPlugin" );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthenticationPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of( SecuritySettings.auth_cache_ttl, Duration.ofMinutes( 60 ) ) );

        TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        // When we log in the first time our plugin should get a call
        assertAuth( "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( "neo4j", "wrong_password", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateWithTestCustomCacheableAuthenticationPlugin() throws Throwable
    {
        TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        restartServerWithOverriddenSettings( Map.of( SecuritySettings.auth_cache_ttl, Duration.ofMinutes( 60 ) ) );

        // When we log in the first time our plugin should get a call
        assertAuth( "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( "neo4j", "wrong_password", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestAuthPlugin()
    {
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithCacheableTestAuthPlugin()
    {
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthPlugin() throws Throwable
    {
        TestCacheableAuthPlugin.getAuthInfoCallCount.set( 0 );

        restartServerWithOverriddenSettings( Map.of( SecuritySettings.auth_cache_ttl, Duration.ofMinutes( 60 ) ) );

        // When we log in the first time our plugin should get a call
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the second time our plugin should _not_ get a call since auth info should be cached
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since auth info should be cached
        assertAuthFail( "neo4j", "wrong_password", "plugin-TestCacheableAuthPlugin" );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestCombinedAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(  authentication_providers, List.of( "plugin-TestCombinedAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCombinedAuthPlugin" ) ) );

        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCombinedAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTwoSeparateTestPlugins() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(  authentication_providers, List.of( "plugin-TestAuthenticationPlugin", "plugin-TestAuthorizationPlugin" ),
                authorization_providers, List.of( "plugin-TestAuthenticationPlugin", "plugin-TestAuthorizationPlugin" ) ) );

        try ( Driver driver = connectDriver( "neo4j", "neo4j" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(  authentication_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ) ) );

        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" ) )
        {
            assertReadSucceeds( driver );

            // When
            clearAuthCacheFromDifferentConnection( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );

            // Then
            assertAuthorizationExpired( driver, "plugin-TestCacheableAdminAuthPlugin" );
        }
    }

    @Test
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(  authentication_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ) ) );

        // Then
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );
                Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CALL dbms.security.clearAuthCache()" );
                assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) );
                tx.success();
            }
        }
    }

    @Test
    public void shouldAuthenticateWithTestCustomParametersAuthenticationPlugin() throws Throwable
    {
        AuthToken token = AuthTokens.custom( "neo4j", "", "plugin-TestCustomParametersAuthenticationPlugin", "custom",
                map( "my_credentials", Arrays.asList( 1L, 2L, 3L, 4L ) ) );
        assertAuth( token );
    }

    @Test
    public void shouldPassOnAuthorizationExpiredException() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(  authentication_providers, List.of( "plugin-TestCombinedAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCombinedAuthPlugin" ) ) );

        try ( Driver driver = connectDriver( "authorization_expired_user", "neo4j" ) )
        {
            assertAuthorizationExpired( driver, "plugin-TestCombinedAuthPlugin" );
        }
    }
}
