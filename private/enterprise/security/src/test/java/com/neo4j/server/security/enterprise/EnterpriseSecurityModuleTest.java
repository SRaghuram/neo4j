/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.server.security.enterprise.EnterpriseSecurityModule.mergeAuthenticationAndAuthorization;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EnterpriseSecurityModuleTest
{
    private Config config;
    private LogProvider mockLogProvider;
    private FileSystemAbstraction mockFileSystem;

    @BeforeEach
    void setup()
    {
        config = mock( Config.class );
        mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        mockFileSystem = mock( FileSystemAbstraction.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.auth_cache_ttl ) ).thenReturn( Duration.ZERO );
        when( config.get( SecuritySettings.auth_cache_max_capacity ) ).thenReturn( 10 );
        when( config.get( SecuritySettings.auth_cache_use_ttl ) ).thenReturn( true );
        when( config.get( SecuritySettings.security_log_successful_authentication ) ).thenReturn( false );
        when( config.get( GraphDatabaseSettings.auth_max_failed_attempts ) ).thenReturn( 3 );
        when( config.get( GraphDatabaseSettings.auth_lock_time ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( config.get( GraphDatabaseSettings.auth_store ) ).thenReturn( Path.of( "mock", "dir" ) );
        when( config.get( DatabaseManagementSystemSettings.auth_store_directory ) ).thenReturn( Path.of( "mock", "dir" ) );
        when( mockFileSystem.fileExists( any() ) ).thenReturn( false );
    }

    @Test
    void shouldFailOnIllegalRealmNameConfiguration()
    {
        // Given
        providers( "this-realm-does-not-exist" );

        // Then
        assertIllegalArgumentException( "Illegal configuration: No authentication provider found." );
    }

    @Test
    void shouldFailOnNoAuthenticationMechanism()
    {
        // Given
        authenticationProviders();
        authorizationProviders( SecuritySettings.NATIVE_REALM_NAME );

        // Then
        assertIllegalArgumentException( "Illegal configuration: No authentication provider found." );
    }

    @Test
    void shouldFailOnNoAuthorizationMechanism()
    {
        // Given
        authenticationProviders( SecuritySettings.NATIVE_REALM_NAME );
        authorizationProviders();

        // Then
        assertIllegalArgumentException( "Illegal configuration: No authorization provider found." );
    }

    @Test
    void shouldFailOnNotLoadedPluginAuthProvider()
    {
        // Given
        providers( SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthenticationPlugin",
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "IllConfiguredAuthorizationPlugin" );

        // Then
        assertIllegalArgumentException( "Illegal configuration: Failed to load auth plugin 'plugin-IllConfiguredAuthorizationPlugin'." );
    }

    @Test
    void shouldNotFailNativeProviderhWithLdapAuthorizationProvider()
    {
        // Given
        providers( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME );

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
        providers( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthorizationPlugin" );

        assertSuccess();
    }

    @Test
    void shouldFailIfPropertyLevelConfigEnabled()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( true );

        assertIllegalArgumentException(
                "Illegal configuration: Property level blacklisting through configuration setting has been replaced by privilege management on roles, e.g. " +
                "'DENY READ {property} ON GRAPH * ELEMENTS * TO role'." );
    }

    @Test
    void shouldNotFailIfPropertyLevelConfigDisabled()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecuritySettings.property_level_authorization_enabled ) ).thenReturn( false );

        assertSuccess();
    }

    @Test
    void shouldFailIfPropertyLevelPermissionsConfigured()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecuritySettings.property_level_authorization_permissions ) ).thenReturn( "smith=alias" );

        assertIllegalArgumentException(
                "Illegal configuration: Property level blacklisting through configuration setting has been replaced by privilege management on roles, e.g. " +
                "'DENY READ {property} ON GRAPH * ELEMENTS * TO role'." );
    }

    @Test
    void shouldNotFailIfPropertyLevelPermissionsNotConfigured()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecuritySettings.property_level_authorization_permissions ) ).thenReturn( null );

        assertSuccess();
    }

    @Test
    void testMerge()
    {
        List<String> merged = mergeAuthenticationAndAuthorization( List.of( "a" ), List.of( "b", "c" ) );
        assertThat( merged, containsInAnyOrder( "a", "b", "c" ) );
        assertThat( merged, containsInRelativeOrder( "b", "c" ) );
        assertThat( merged.size(), is( 3 ) );

        merged = mergeAuthenticationAndAuthorization( List.of( "a", "b" ), List.of( "b", "c" ) );
        assertThat( merged, containsInRelativeOrder( "a", "b", "c" ) );
        assertThat( merged.size(), is( 3 ) );

        merged = mergeAuthenticationAndAuthorization( List.of( "a", "b", "d" ), List.of( "b", "c", "d" ) );
        assertThat( merged, containsInRelativeOrder( "a", "b", "c", "d" ) );
        assertThat( merged.size(), is( 4 ) );

        merged = mergeAuthenticationAndAuthorization( List.of( "a", "b", "c" ), List.of() );
        assertThat( merged, containsInRelativeOrder( "a", "b", "c" ) );
        assertThat( merged.size(), is( 3 ) );

        merged = mergeAuthenticationAndAuthorization( List.of(), List.of("a", "b", "c" ) );
        assertThat( merged, containsInRelativeOrder( "a", "b", "c" ) );
        assertThat( merged.size(), is( 3 ) );

        merged = mergeAuthenticationAndAuthorization( List.of(), List.of() );
        assertThat( merged.size(), is( 0 ) );

        IllegalArgumentException illegalArgumentException =
                assertThrows( IllegalArgumentException.class, () -> mergeAuthenticationAndAuthorization( List.of( "a", "b" ), List.of( "b", "a" ) ) );
        assertEquals( "Illegal configuration: The relative order of authentication providers and authorization providers must match.",
                illegalArgumentException.getMessage() );
    }

    @RepeatedTest( 100 )
    void testMergeRandom()
    {
        Random random = new Random();

        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        List<String> r = new ArrayList<>();
        for ( int i = 'a'; i <= 'z'; i++ )
        {
            String c = Character.toString( i );
            switch ( random.nextInt( 3 ) )
            {
            case 0:
                a.add( c );
                break;
            case 1:
                b.add( c );
                break;
            case 2:
                a.add( c );
                b.add( c );
                break;
            default:
                throw new RuntimeException( "?!" );
            }
            r.add( c );
        }
        List<String> merged = mergeAuthenticationAndAuthorization( a, b );
        assertThat( merged.size(), is( r.size() ) );
        assertThat( merged, containsInRelativeOrder( a.toArray() ) );
        assertThat( merged, containsInRelativeOrder( b.toArray() ) );
    }

    // --------- HELPERS ----------
    private void providers( String... providers )
    {
        authenticationProviders( providers );
        authorizationProviders( providers );
    }

    private void authenticationProviders( String... providers )
    {
        when( config.get( SecuritySettings.authentication_providers ) ).thenReturn( Arrays.asList( providers ) );
    }

    private void authorizationProviders( String... providers )
    {
        when( config.get( SecuritySettings.authorization_providers ) ).thenReturn( Arrays.asList( providers ) );
    }

    private void assertSuccess()
    {
        new EnterpriseSecurityModule( mockLogProvider, config, mock( GlobalProcedures.class ), mock( JobScheduler.class ), mockFileSystem,
                mock( DependencySatisfier.class ), mock( GlobalTransactionEventListeners.class ) ).newAuthManager( mock( SecurityLog.class ) );
    }

    private void assertIllegalArgumentException( String errorMsg )
    {
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class,
                () -> new EnterpriseSecurityModule( mockLogProvider, config, mock( GlobalProcedures.class ), mock( JobScheduler.class ), mockFileSystem,
                        mock( DependencySatisfier.class ), mock( GlobalTransactionEventListeners.class ) ).newAuthManager( mock( SecurityLog.class ) ) );
        assertEquals( e.getMessage(), errorMsg );
    }
}
