/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthenticationPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.TestCustomCacheableAuthenticationPlugin;
import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.SecuritySettings.PLUGIN_REALM_NAME_PREFIX;
import static com.neo4j.configuration.SecuritySettings.authentication_providers;
import static com.neo4j.configuration.SecuritySettings.authorization_providers;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuthFail;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuthorizationExpired;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertReadSucceeds;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertWriteFails;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.clearAuthCacheFromDifferentConnection;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.connectDriver;
import static org.apache.commons.lang3.StringUtils.prependIfMissing;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class PluginAuthenticationIT
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

    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    private DbmsRule dbRule = new EnterpriseDbmsRule( testDirectory ).startLazily();

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    private String boltUri;

    private void startDatabase()
    {
        startDatabaseWithSettings( Collections.emptyMap() );
    }

    private void startDatabaseWithSettings( Map<Setting<?>,Object> settings )
    {
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, true )
                .withSetting( BoltConnector.enabled, true )
                .withSetting( BoltConnector.encryption_level, DISABLED )
                .withSetting( BoltConnector.listen_address, new SocketAddress( InetAddress.getLoopbackAddress().getHostAddress(), 0 ) )
                .withSetting( authentication_providers, DEFAULT_TEST_PLUGIN_REALMS )
                .withSetting( authorization_providers, DEFAULT_TEST_PLUGIN_REALMS );
        dbRule.withSettings( settings );
        dbRule.ensureStarted();
        boltUri = DriverAuthHelper.boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );
        try ( org.neo4j.graphdb.Transaction tx = dbRule.beginTx() )
        {
            // create a node to be able to assert that access without other privileges sees empty graph
            tx.createNode();
            tx.commit();
        }
    }

    @Test
    public void shouldAuthenticateWithTestAuthenticationPlugin()
    {
        startDatabase();
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestAuthenticationPlugin" );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthenticationPlugin()
    {
        startDatabaseWithSettings( Map.of( SecuritySettings.auth_cache_ttl, Duration.ofMinutes( 60 ) ) );

        TestCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.set( 0 );

        // When we log in the first time our plugin should get a call
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( boltUri, "neo4j", "wrong_password", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateWithTestCustomCacheableAuthenticationPlugin()
    {
        TestCustomCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.set( 0 );

        startDatabaseWithSettings( Map.of( SecuritySettings.auth_cache_ttl, Duration.ofMinutes( 60 ) ) );

        // When we log in the first time our plugin should get a call
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( boltUri, "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( boltUri, "neo4j", "wrong_password", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.GET_AUTHENTICATION_INFO_CALL_COUNT.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestAuthPlugin()
    {
        startDatabase();
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithCacheableTestAuthPlugin()
    {
        startDatabase();
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthPlugin()
    {
        TestCacheableAuthPlugin.GET_AUTH_INFO_CALL_COUNT.set( 0 );

        startDatabaseWithSettings( Map.of( SecuritySettings.auth_cache_ttl, Duration.ofMinutes( 60 ) ) );

        // When we log in the first time our plugin should get a call
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.GET_AUTH_INFO_CALL_COUNT.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the second time our plugin should _not_ get a call since auth info should be cached
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.GET_AUTH_INFO_CALL_COUNT.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since auth info should be cached
        assertAuthFail( boltUri, "neo4j", "wrong_password", "plugin-TestCacheableAuthPlugin" );
        assertThat( TestCacheableAuthPlugin.GET_AUTH_INFO_CALL_COUNT.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestCombinedAuthPlugin()
    {
        startDatabaseWithSettings( Map.of(  authentication_providers, List.of( "plugin-TestCombinedAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCombinedAuthPlugin" ) ) );

        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCombinedAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTwoSeparateTestPlugins()
    {
        startDatabaseWithSettings( Map.of(  authentication_providers, List.of( "plugin-TestAuthenticationPlugin", "plugin-TestAuthorizationPlugin" ),
                authorization_providers, List.of( "plugin-TestAuthenticationPlugin", "plugin-TestAuthorizationPlugin" ) ) );

        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithAuthPlugin()
    {
        startDatabaseWithSettings( Map.of(  authentication_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ) ) );

        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" ) )
        {
            assertReadSucceeds( driver );

            // When
            clearAuthCacheFromDifferentConnection( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );

            // Then
            assertAuthorizationExpired( driver, "plugin-TestCacheableAdminAuthPlugin" );
        }
    }

    @Test
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithAuthPlugin()
    {
        startDatabaseWithSettings( Map.of(  authentication_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCacheableAdminAuthPlugin" ) ) );

        // Then
        try ( Driver driver = connectDriver( boltUri, "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );
                Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CALL dbms.security.clearAuthCache()" );
                assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) );
                tx.commit();
            }
        }
    }

    @Test
    public void shouldAuthenticateWithTestCustomParametersAuthenticationPlugin()
    {
        startDatabase();
        AuthToken token = AuthTokens.custom( "neo4j", "", "plugin-TestCustomParametersAuthenticationPlugin", "custom",
                map( "my_credentials", Arrays.asList( 1L, 2L, 3L, 4L ) ) );
        assertAuth( boltUri, token );
    }

    @Test
    public void shouldPassOnAuthorizationExpiredException()
    {
        startDatabaseWithSettings( Map.of(  authentication_providers, List.of( "plugin-TestCombinedAuthPlugin" ),
                authorization_providers, List.of( "plugin-TestCombinedAuthPlugin" ) ) );

        try ( Driver driver = connectDriver( boltUri, "authorization_expired_user", "neo4j" ) )
        {
            assertAuthorizationExpired( driver, "plugin-TestCombinedAuthPlugin" );
        }
    }
}
