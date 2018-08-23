/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommercialSecurityModuleTest
{
    private Config config;
    private LogProvider mockLogProvider;

    @BeforeEach
    void setup()
    {
        config = mock( Config.class );
        mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.auth_cache_ttl ) ).thenReturn( Duration.ZERO );
        when( config.get( SecuritySettings.auth_cache_max_capacity ) ).thenReturn( 10 );
        when( config.get( SecuritySettings.auth_cache_use_ttl ) ).thenReturn( true );
        when( config.get( SecuritySettings.security_log_successful_authentication ) ).thenReturn( false );
        when( config.get( GraphDatabaseSettings.auth_max_failed_attempts ) ).thenReturn( 3 );
        when( config.get( GraphDatabaseSettings.auth_lock_time ) ).thenReturn( Duration.ofSeconds( 5 ) );
    }

    @Test
    void shouldFailOnIllegalRealmNameConfiguration()
    {
        // Given
        systemGraphAuth( true, true );
        ldapAuth( true, true );
        pluginAuth( false, false );
        authProviders( "this-realm-does-not-exist" );

        // Then
        assertIllegalArgumentException( "Illegal configuration: No valid auth provider is active." );
    }

    @Test
    void shouldFailOnNoAuthenticationMechanism()
    {
        // Given
        systemGraphAuth( false, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );

        // Then
        assertIllegalArgumentException( "Illegal configuration: All authentication providers are disabled." );
    }

    @Test
    void shouldFailOnNoAuthorizationMechanism()
    {
        // Given
        systemGraphAuth( true, false );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );

        // Then
        assertIllegalArgumentException( "Illegal configuration: All authorization providers are disabled." );
    }

    @Test
    void shouldFailOnIllegalAdvancedRealmConfiguration()
    {
        // Given
        systemGraphAuth( false, false );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders( SecuritySettings.SYSTEM_GRAPH_REALM_NAME, SecuritySettings.LDAP_REALM_NAME );

        // Then
        assertIllegalArgumentException( "Illegal configuration: System-graph auth provider configured, " +
                "but both authentication and authorization are disabled." );
    }

    @Test
    void shouldFailOnNotLoadedPluginAuthProvider()
    {
        // Given
        systemGraphAuth( false, false );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders(
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthenticationPlugin",
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "IllConfiguredAuthorizationPlugin"
        );

        // Then
        assertIllegalArgumentException( "Illegal configuration: Failed to load auth plugin 'plugin-IllConfiguredAuthorizationPlugin'." );
    }

    @Test
    void shouldNotFailWithOnlySystemGraphProvider()
    {
        // Given
        systemGraphAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );

        authProviders(
                SecuritySettings.SYSTEM_GRAPH_REALM_NAME
        );

        // Then
        assertSuccess();
    }

    @Test
    void shouldFailWithNativeProviderAndSystemGraphProviderTogether()
    {
        // Given
        systemGraphAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );

        authProviders(
                SecuritySettings.SYSTEM_GRAPH_REALM_NAME,
                SecuritySettings.NATIVE_REALM_NAME
        );

        // Then
        assertIllegalArgumentException( "Illegal configuration: Both system-graph auth provider and native auth provider configured," +
                " but they cannot be used together. Please remove one of them from the configuration." );
    }

    @Test
    void shouldNotFailSystemGraphProviderhWithLdapAuthorizationProvider()
    {
        // Given
        systemGraphAuth( true, true );
        ldapAuth( true, true );
        pluginAuth( false, false );
        authProviders(
                SecuritySettings.SYSTEM_GRAPH_REALM_NAME,
                SecuritySettings.LDAP_REALM_NAME
        );

        // When
        when( config.get( SecuritySettings.ldap_connection_timeout ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( config.get( SecuritySettings.ldap_read_timeout ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( config.get( SecuritySettings.ldap_authorization_connection_pooling ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_use_samaccountname ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_cache_enabled  ) ).thenReturn( false );

        // Then
        assertSuccess();
    }

    @Test
    void shouldNotFailSystemGraphProviderWithPluginAuthorizationProvider()
    {
        // Given
        systemGraphAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders(
                SecuritySettings.SYSTEM_GRAPH_REALM_NAME,
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthorizationPlugin"
        );

        // Then
        assertSuccess();
    }

    // --------- HELPERS ----------

    private void systemGraphAuth( boolean authn, boolean authr )
    {
        when( config.get( SecuritySettings.native_authentication_enabled ) ).thenReturn( authn );
        when( config.get( SecuritySettings.native_authorization_enabled ) ).thenReturn( authr );
    }

    private void ldapAuth( boolean authn, boolean authr )
    {
        when( config.get( SecuritySettings.ldap_authentication_enabled ) ).thenReturn( authn );
        when( config.get( SecuritySettings.ldap_authorization_enabled ) ).thenReturn( authr );
    }

    private void pluginAuth( boolean authn, boolean authr )
    {
        when( config.get( SecuritySettings.plugin_authentication_enabled ) ).thenReturn( authn );
        when( config.get( SecuritySettings.plugin_authorization_enabled ) ).thenReturn( authr );
    }

    private void authProviders( String... authProviders )
    {
        when( config.get( SecuritySettings.auth_providers ) ).thenReturn( Arrays.asList( authProviders ) );
    }

    private void assertSuccess()
    {
        new CommercialSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    private void assertIllegalArgumentException( String errorMsg )
    {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new CommercialSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null ) );
        assertEquals( e.getMessage(), errorMsg );
    }
}
