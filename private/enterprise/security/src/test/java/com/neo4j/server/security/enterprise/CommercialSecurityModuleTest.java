/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommercialSecurityModuleTest
{
    private Config config;
    private LogProvider mockLogProvider;
    private FileSystemAbstraction mockFileSystem;
    private AccessCapability mockAccessCapability;

    @BeforeEach
    void setup()
    {
        config = mock( Config.class );
        mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        mockFileSystem = mock( FileSystemAbstraction.class );
        mockAccessCapability = mock( AccessCapability.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.auth_cache_ttl ) ).thenReturn( Duration.ZERO );
        when( config.get( SecuritySettings.auth_cache_max_capacity ) ).thenReturn( 10 );
        when( config.get( SecuritySettings.auth_cache_use_ttl ) ).thenReturn( true );
        when( config.get( SecuritySettings.security_log_successful_authentication ) ).thenReturn( false );
        when( config.get( GraphDatabaseSettings.auth_max_failed_attempts ) ).thenReturn( 3 );
        when( config.get( GraphDatabaseSettings.auth_lock_time ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( mockFileSystem.fileExists( any() ) ).thenReturn( false );
    }

    @Test
    void shouldFailOnIllegalRealmNameConfiguration()
    {
        // Given
        nativeAuth( true, true );
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
        nativeAuth( false, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        // Then
        assertIllegalArgumentException( "Illegal configuration: All authentication providers are disabled." );
    }

    @Test
    void shouldFailOnNoAuthorizationMechanism()
    {
        // Given
        nativeAuth( true, false );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        // Then
        assertIllegalArgumentException( "Illegal configuration: All authorization providers are disabled." );
    }

    @Test
    void shouldFailOnIllegalAdvancedRealmConfiguration()
    {
        // Given
        nativeAuth( false, false );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME );

        // Then
        assertIllegalArgumentException(
                "Illegal configuration: Native auth provider configured, but both authentication and authorization are disabled." );
    }

    @Test
    void shouldFailOnNotLoadedPluginAuthProvider()
    {
        // Given
        nativeAuth( false, false );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders( SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthenticationPlugin",
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "IllConfiguredAuthorizationPlugin" );

        // Then
        assertIllegalArgumentException( "Illegal configuration: Failed to load auth plugin 'plugin-IllConfiguredAuthorizationPlugin'." );
    }

    @Test
    void shouldTranslateFromSystemGraphProviderToNative()
    {
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );

        CommercialSecurityModule.SecurityConfig securityConfig = new CommercialSecurityModule.SecurityConfig( config );
        securityConfig.validate();

        assertThat( securityConfig.hasNativeProvider, equalTo( true ) );
    }

    @Test
    void shouldTranslateWithNativeProviderAndSystemGraphProviderTogether()
    {
        // Given
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );

        authProviders( SecuritySettings.SYSTEM_GRAPH_REALM_NAME, SecuritySettings.NATIVE_REALM_NAME );

        CommercialSecurityModule.SecurityConfig securityConfig = new CommercialSecurityModule.SecurityConfig( config );
        securityConfig.validate();

        // Then
        assertThat( securityConfig.hasNativeProvider, equalTo( true ) );
    }

    @Test
    void shouldNotFailNativeProviderhWithLdapAuthorizationProvider()
    {
        // Given
        nativeAuth( true, true );
        ldapAuth( true, true );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME );

        // When
        when( config.get( SecuritySettings.ldap_connection_timeout ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( config.get( SecuritySettings.ldap_read_timeout ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( config.get( SecuritySettings.ldap_authorization_connection_pooling ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_use_samaccountname ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_cache_enabled ) ).thenReturn( false );

        // Then
        assertSuccess();
    }

    @Test
    void shouldNotFailNativeWithPluginAuthorizationProvider()
    {
        // Given
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthorizationPlugin" );

        assertSuccess();
    }

    @Test
    void shouldNotFailWithPropertyLevelPermissions()
    {
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.property_level_authorization_permissions ) ).thenReturn( "smith=alias" );

        assertSuccess();
    }

    @Test
    void shouldFailOnIllegalPropertyLevelPermissions()
    {
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.property_level_authorization_permissions ) ).thenReturn( "smithmalias" );

        assertIllegalArgumentException( "Illegal configuration: Property level authorization is enabled but there is a error in the permissions mapping." );
    }

    @Test
    void shouldParsePropertyLevelPermissions()
    {
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.property_level_authorization_permissions ) ).thenReturn(
                "smith = alias;merovingian=alias ,location;\n abel=alias,\t\thasSilver" );

        CommercialSecurityModule.SecurityConfig securityConfig = new CommercialSecurityModule.SecurityConfig( config );
        securityConfig.validate();
        assertThat( securityConfig.propertyBlacklist.get( "smith" ), equalTo( Collections.singletonList( "alias" ) ) );
        assertThat( securityConfig.propertyBlacklist.get( "merovingian" ), equalTo( Arrays.asList( "alias", "location" ) ) );
        assertThat( securityConfig.propertyBlacklist.get( "abel" ), equalTo( Arrays.asList( "alias", "hasSilver" ) ) );
    }

    // --------- HELPERS ----------
    private void nativeAuth( boolean authn, boolean authr )
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
        new CommercialSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class ), mockFileSystem, mockAccessCapability );
    }

    private void assertIllegalArgumentException( String errorMsg )
    {
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class,
                () -> new CommercialSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class ), mockFileSystem,
                        mockAccessCapability ) );
        assertEquals( e.getMessage(), errorMsg );
    }
}
