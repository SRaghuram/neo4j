/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.configuration.SecurityInternalSettings;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.auth.SecurityProcedures;
import com.neo4j.server.security.enterprise.auth.TestExecutorCaffeineCacheFactory;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.TestDefaultDatabaseResolver;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.server.security.enterprise.EnterpriseSecurityModule.mergeAuthenticationAndAuthorization;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

class EnterpriseSecurityModuleTest
{
    private Config config;
    private LogProvider mockLogProvider;
    private SecurityLog mockSecurityLog;
    private Supplier<GraphDatabaseService> mockSystemSupplier;
    private GlobalProcedures mockProcedures;
    private GlobalTransactionEventListeners mockEventListeners;
    private Dependencies mockDependencies;
    private EnterpriseSecurityGraphComponent mockSecurityComponent;
    private CaffeineCacheFactory caffeineCacheFactory;

    @BeforeEach
    void setup()
    {
        config = mock( Config.class );
        mockLogProvider = mock( LogProvider.class );
        mockSecurityLog = mock( SecurityLog.class );
        mockSystemSupplier = () -> mock( GraphDatabaseService.class );
        mockSecurityComponent = mock( EnterpriseSecurityGraphComponent.class );
        Log mockLog = mock( Log.class );
        mockProcedures = mock( GlobalProcedures.class );
        mockEventListeners = mock( GlobalTransactionEventListeners.class );
        mockDependencies = new Dependencies();
        mockDependencies.satisfyDependency( mockProcedures );
        caffeineCacheFactory = TestExecutorCaffeineCacheFactory.getInstance();
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecurityInternalSettings.property_level_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.auth_cache_ttl ) ).thenReturn( Duration.ZERO );
        when( config.get( SecuritySettings.auth_cache_max_capacity ) ).thenReturn( 10 );
        when( config.get( SecuritySettings.auth_cache_use_ttl ) ).thenReturn( true );
        when( config.get( SecuritySettings.security_log_successful_authentication ) ).thenReturn( false );
        when( config.get( GraphDatabaseSettings.auth_max_failed_attempts ) ).thenReturn( 3 );
        when( config.get( GraphDatabaseSettings.auth_lock_time ) ).thenReturn( Duration.ofSeconds( 5 ) );
        when( config.get( GraphDatabaseInternalSettings.auth_store ) ).thenReturn( Path.of( "mock", "dir" ) );
        when( config.get( DatabaseManagementSystemSettings.auth_store_directory ) ).thenReturn( Path.of( "mock", "dir" ) );
        when( config.get( BoltConnectorInternalSettings.enable_loopback_auth ) ).thenReturn( false );
        when( config.get( GraphDatabaseSettings.procedure_roles ) ).thenReturn( "" );
        when( config.get( GraphDatabaseSettings.default_allowed ) ).thenReturn( "" );
    }

    @Test
    void shouldFailDatabaseCreationIfNotAbleToLoadSecurityProcedures() throws KernelException
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        doThrow( new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, "Injected error" ) )
                .when( mockProcedures ).registerProcedure( SecurityProcedures.class, true, null );

        var securityModule = createModule( logProvider, Config.defaults() );

        // When
        RuntimeException runtimeException = assertThrows( RuntimeException.class, securityModule::setup );

        // Then
        String errorMessage = "Failed to register security procedures: Injected error";
        assertThat( runtimeException.getMessage() ).isEqualTo( errorMessage );
        assertThat( runtimeException.getCause() ).isInstanceOf( KernelException.class );

        assertThat( logProvider ).forClass( EnterpriseSecurityModule.class ).forLevel( ERROR )
                .assertExceptionForLogMessage( errorMessage ).isInstanceOf( KernelException.class );
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
        when( config.get( SecurityInternalSettings.ldap_authorization_connection_pooling ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_use_attribute ) ).thenReturn( false );
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
        when( config.get( SecurityInternalSettings.property_level_authorization_enabled ) ).thenReturn( true );

        assertIllegalArgumentException(
                "Illegal configuration: Property level blacklisting through configuration setting has been replaced by privilege management on roles, e.g. " +
                "'DENY READ {property} ON GRAPH * ELEMENTS * TO role'." );
    }

    @Test
    void shouldNotFailIfPropertyLevelConfigDisabled()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecurityInternalSettings.property_level_authorization_enabled ) ).thenReturn( false );

        assertSuccess();
    }

    @Test
    void shouldFailIfPropertyLevelPermissionsConfigured()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecurityInternalSettings.property_level_authorization_permissions ) ).thenReturn( "smith=alias" );

        assertIllegalArgumentException(
                "Illegal configuration: Property level blacklisting through configuration setting has been replaced by privilege management on roles, e.g. " +
                "'DENY READ {property} ON GRAPH * ELEMENTS * TO role'." );
    }

    @Test
    void shouldNotFailIfPropertyLevelPermissionsNotConfigured()
    {
        providers( SecuritySettings.NATIVE_REALM_NAME );
        when( config.get( SecurityInternalSettings.property_level_authorization_permissions ) ).thenReturn( null );

        assertSuccess();
    }

    @Test
    void testMerge()
    {
        List<String> merged = mergeAuthenticationAndAuthorization( List.of( "a" ), List.of( "b", "c" ) );
        assertThat( merged ).contains( "a", "b", "c" );
        assertThat( merged ).containsSequence( "b", "c" );
        assertThat( merged.size() ).isEqualTo( 3 );

        merged = mergeAuthenticationAndAuthorization( List.of( "a", "b" ), List.of( "b", "c" ) );
        assertThat( merged ).containsSequence( "a", "b", "c" );
        assertThat( merged.size() ).isEqualTo( 3 );

        merged = mergeAuthenticationAndAuthorization( List.of( "a", "b", "d" ), List.of( "b", "c", "d" ) );
        assertThat( merged ).containsSequence( "a", "b", "c", "d" );
        assertThat( merged.size() ).isEqualTo( 4 );

        merged = mergeAuthenticationAndAuthorization( List.of( "a", "b", "c" ), List.of() );
        assertThat( merged ).containsSequence( "a", "b", "c" );
        assertThat( merged.size() ).isEqualTo( 3 );

        merged = mergeAuthenticationAndAuthorization( List.of(), List.of("a", "b", "c" ) );
        assertThat( merged ).containsSequence( "a", "b", "c" );
        assertThat( merged.size() ).isEqualTo( 3 );

        merged = mergeAuthenticationAndAuthorization( List.of(), List.of() );
        assertThat( merged.size() ).isEqualTo( 0 );

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
        assertThat( merged.size() ).isEqualTo( r.size() );
        assertThat( merged ).containsSubsequence( a );
        assertThat( merged ).containsSubsequence( b );
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
        createModule( mockLogProvider, config ).newAuthManager( mock( SecurityLog.class ), mockSystemSupplier );
    }

    private void assertIllegalArgumentException( String errorMsg )
    {
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class,
                () -> createModule( mockLogProvider, config ).newAuthManager( mock( SecurityLog.class ), mockSystemSupplier ) );
        assertEquals( e.getMessage(), errorMsg );
    }

    private EnterpriseSecurityModule createModule( LogProvider logProvider, Config config )
    {
        return new EnterpriseSecurityModule( logProvider, mockSecurityLog, config, mockDependencies, mockEventListeners, mockSecurityComponent,
                caffeineCacheFactory, new EphemeralFileSystemAbstraction(), new TestDefaultDatabaseResolver( "neo4j" ) );
    }
}
