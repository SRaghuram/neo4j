/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCustomCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuthFail;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuthorizationExpired;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertReadSucceeds;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertWriteFails;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.clearAuthCacheFromDifferentConnection;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.connectDriver;

public class PluginAuthenticationIT
{
    private TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    private DatabaseRule dbRule = new EnterpriseDatabaseRule( testDirectory ).startLazily();

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    private static final List<String> defaultTestPluginRealmList = Arrays.asList(
            "TestAuthenticationPlugin",
            "TestAuthPlugin",
            "TestCacheableAdminAuthPlugin",
            "TestCacheableAuthenticationPlugin",
            "TestCacheableAuthPlugin",
            "TestCustomCacheableAuthenticationPlugin",
            "TestCustomParametersAuthenticationPlugin"
    );

    private static final String DEFAULT_TEST_PLUGIN_REALMS = String.join( ", ",
            defaultTestPluginRealmList.stream()
                    .map( s -> StringUtils.prependIfMissing( s, SecuritySettings.PLUGIN_REALM_NAME_PREFIX ) )
                    .collect( Collectors.toList() )
    );

    @Before
    public void setup() throws Exception
    {
        String host = InetAddress.getLoopbackAddress().getHostAddress() + ":0";
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, "true" )
                .withSetting( new BoltConnector( "bolt" ).type, "BOLT" )
                .withSetting( new BoltConnector( "bolt" ).enabled, "true" )
                .withSetting( new BoltConnector( "bolt" ).encryption_level, OPTIONAL.name() )
                .withSetting( new BoltConnector( "bolt" ).listen_address, host )
                .withSetting( SecuritySettings.auth_providers, DEFAULT_TEST_PLUGIN_REALMS );
        dbRule.ensureStarted();
        boltUri = DriverAuthHelper.boltUri( dbRule );
    }

    private String boltUri;

    private void restartServerWithOverriddenSettings( String... configChanges ) throws IOException
    {
        dbRule.restartDatabase( configChanges );
        boltUri = DriverAuthHelper.boltUri( dbRule );
    }

    @Test
    public void shouldAuthenticateWithTestAuthenticationPlugin()
    {
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestAuthenticationPlugin" );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthenticationPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_cache_ttl.name(), "60m" );

        TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        // When we log in the first time our plugin should get a call
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( boltUri, "neo4j", "wrong_password", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateWithTestCustomCacheableAuthenticationPlugin() throws Throwable
    {
        TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        restartServerWithOverriddenSettings( SecuritySettings.auth_cache_ttl.name(), "60m" );

        // When we log in the first time our plugin should get a call
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( boltUri, "neo4j", "wrong_password", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestAuthPlugin()
    {
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithCacheableTestAuthPlugin()
    {
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthPlugin() throws Throwable
    {
        TestCacheableAuthPlugin.getAuthInfoCallCount.set( 0 );

        restartServerWithOverriddenSettings( SecuritySettings.auth_cache_ttl.name(), "60m" );

        // When we log in the first time our plugin should get a call
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the second time our plugin should _not_ get a call since auth info should be cached
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since auth info should be cached
        assertAuthFail( boltUri, "neo4j", "wrong_password", "plugin-TestCacheableAuthPlugin" );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestCombinedAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCombinedAuthPlugin" );

        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCombinedAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTwoSeparateTestPlugins() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestAuthenticationPlugin,plugin-TestAuthorizationPlugin" );

        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCacheableAdminAuthPlugin" );

        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" ) )
        {
            assertReadSucceeds( driver );

            // When
            clearAuthCacheFromDifferentConnection( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );

            // Then
            assertAuthorizationExpired( driver );
        }
    }

    @Test
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCacheableAdminAuthPlugin" );

        // Then
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );
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
    public void shouldAuthenticateWithTestCustomParametersAuthenticationPlugin()
    {
        AuthToken token = AuthTokens.custom( "neo4j", "", "plugin-TestCustomParametersAuthenticationPlugin", "custom",
                map( "my_credentials", Arrays.asList( 1L, 2L, 3L, 4L ) ) );
        assertAuth( boltUri, token );
    }

    @Test
    public void shouldPassOnAuthorizationExpiredException() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCombinedAuthPlugin" );

        try ( Driver driver = connectDriver( boltUri, "authorization_expired_user", "neo4j" ) )
        {
            assertAuthorizationExpired( driver );
        }
    }
}
