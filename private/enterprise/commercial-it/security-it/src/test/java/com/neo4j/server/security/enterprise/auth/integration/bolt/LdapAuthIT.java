/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.auth.LdapRealm;
import com.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.plugin.LdapGroupHasUsersAuthPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapOperationErrorException;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.api.filtering.EntryFilteringCursor;
import org.apache.directory.server.core.api.interceptor.BaseInterceptor;
import org.apache.directory.server.core.api.interceptor.Interceptor;
import org.apache.directory.server.core.api.interceptor.context.SearchOperationContext;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.ldap.LdapContext;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.diagnostics.providers.ConfigDiagnostics;
import org.neo4j.logging.Logger;
import org.neo4j.string.SecureString;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings( "deprecation" )
@RunWith( FrameworkRunner.class )
@CreateDS(
        name = "LdapAuthTest",
        partitions = {@CreatePartition(
                name = "example",
                suffix = "dc=example,dc=com",
                contextEntry = @ContextEntry( entryLdif = "dn: dc=example,dc=com\n" +
                                                          "dc: example\n" +
                                                          "o: example\n" +
                                                          "objectClass: top\n" +
                                                          "objectClass: dcObject\n" +
                                                          "objectClass: organization\n\n" ) ),
        },
        loadedSchemas = {
                @LoadSchema( name = "nis" ),
        } )
@CreateLdapServer(
        transports = {@CreateTransport( protocol = "LDAP", address = "0.0.0.0" ),
                @CreateTransport( protocol = "LDAPS", address = "0.0.0.0", ssl = true )
        },

        saslMechanisms = {
                @SaslMechanism( name = "DIGEST-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name = "CRAM-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = {StartTlsHandler.class},
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( {"ad_schema.ldif", "ldap_test_data.ldif"} )
public class LdapAuthIT extends EnterpriseAuthenticationTestBase
{
    private static final String LDAP_ERROR_MESSAGE_INVALID_CREDENTIALS = "LDAP: error code 49 - INVALID_CREDENTIALS";
    private static final String REFUSED_IP = "127.0.0.1"; // "0.6.6.6";
    private int ldapPort;
    private int sslLdapPort;

    @Before
    @Override
    public void setup() throws Exception
    {
        LdapServer ldapServer = getLdapServer();
        ldapPort = ldapServer.getPort();
        sslLdapPort = ldapServer.getPortSSL();
        ldapServer.setConfidentialityRequired( false );
        super.setup();
        checkIfLdapServerIsReachable( ldapServer.getSaslHost(), ldapPort );
    }

    @Override
    protected Map<Setting<?>,Object> getSettings()
    {
        Map<Setting<?>,Object> settings = new HashMap<>();
        settings.put( SecuritySettings.authentication_providers, List.of( SecuritySettings.LDAP_REALM_NAME ) );
        settings.put( SecuritySettings.authorization_providers, List.of( SecuritySettings.LDAP_REALM_NAME ) );
        settings.put( SecuritySettings.ldap_server, "0.0.0.0:" + ldapPort );
        settings.put( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authentication_cache_enabled, true );
        settings.put( SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system" );
        settings.put( SecuritySettings.ldap_authorization_system_password, new SecureString( "secret" ) );
        settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
        settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, List.of( "gidnumber" ) );
        settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin" );
        settings.put( GraphDatabaseSettings.procedure_roles, "test.staticReadProcedure:role1" );
        settings.put( SecuritySettings.ldap_read_timeout, Duration.ofSeconds( 1 ) );
        settings.put( SecuritySettings.ldap_authorization_use_system_account, false );
        return settings;
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldShowCurrentUser()
    {
        try ( Driver driver = connectDriver( "smith", "abc123" );
                Session session = driver.session() )
        {
            // when
            Record record = session.run( "CALL dbms.showCurrentUser()" ).single();

            // then
            // Assuming showCurrentUser has fields username, roles, flags
            assertThat( record.get( 0 ).asString(), equalTo( "smith" ) );
            assertThat( record.get( 1 ).asList(), equalTo( Collections.emptyList() ) );
            assertThat( record.get( 2 ).asList(), equalTo( Collections.emptyList() ) );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithLdapOnlyAndNoGroupToRoleMapping() throws IOException
    {
        restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_authorization_group_to_role_mapping, "" ) );
        // Then
        // User 'neo' has reader role by default, but since we are not passing a group-to-role mapping
        // he should get no permissions
        assertReadFails( "neo", "abc123" );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldFailIfAuthorizationExpiredWithserLdapContext()
    {
        // Given
        try ( Driver driver = connectDriver( "neo4j", "abc123" ) )
        {
            assertReadSucceeds( driver );

            try ( Session session = driver.session() )
            {
                session.run( "CALL dbms.security.clearAuthCache()" );
            }

            try ( Session session = driver.session() )
            {
                session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
                fail( "should have failed due to authorization expired" );
            }
            catch ( ClientException e )
            {
                assertThat( e.getMessage(), containsString( "LDAP authorization info expired." ) );
            }
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithUserLdapContext()
    {
        // Given
        try ( Driver driver = connectDriver( "neo4j", "abc123" ) )
        {
            assertReadSucceeds( driver );

            try ( Session session = driver.session() )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CALL dbms.security.clearAuthCache()" );
                    assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) );
                    tx.success();
                }
            }
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldKeepAuthorizationForLifetimeOfTransaction() throws Throwable
    {
        assertKeepAuthorizationForLifetimeOfTransaction( "neo",
                tx -> assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) ) );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldKeepAuthorizationForLifetimeOfTransactionWithProcedureAllowed() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_authorization_group_to_role_mapping, "503=admin;504=role1" ) );
        dbRule.resolveDependency( GlobalProcedures.class ).registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );
        assertKeepAuthorizationForLifetimeOfTransaction( "smith",
                tx -> assertThat( tx.run( "CALL test.staticReadProcedure()" ).single().get( 0 ).asString(), equalTo( "static" ) ) );
    }

    private void assertKeepAuthorizationForLifetimeOfTransaction( String username, Consumer<Transaction> assertion ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        final Throwable[] threadFail = {null};

        Thread readerThread = new Thread( () ->
        {
            try
            {
                try ( Driver driver = connectDriver( username, "abc123" );
                        Session session = driver.session();
                        Transaction tx = session.beginTransaction() )
                {
                    assertion.accept( tx );
                    latch.startAndWaitForAllToStart();
                    latch.finishAndWaitForAllToFinish();
                    assertion.accept( tx );
                    tx.success();
                }
            }
            catch ( Throwable t )
            {
                threadFail[0] = t;
                // Always release the latch so we get the failure in the main thread
                latch.start();
                latch.finish();
            }
        } );

        readerThread.start();
        latch.startAndWaitForAllToStart();

        clearAuthCacheFromDifferentConnection();

        latch.finishAndWaitForAllToFinish();

        readerThread.join();
        if ( threadFail[0] != null )
        {
            throw threadFail[0];
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldFailIfInvalidLdapServer() throws IOException
    {
        // When
        restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_server, "ldap://127.0.0.1" ) );
        try
        {
            connectDriver( "neo", "abc123" );
            fail( "should have refused connection" );
        }
        catch ( TransientException e )
        {
            assertThat( e.getMessage(), equalTo( LdapRealm.LDAP_CONNECTION_REFUSED_CLIENT_MESSAGE ) );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldTimeoutIfLdapServerDoesNotRespond() throws IOException
    {
        try ( DirectoryServiceWaitOnSearch ignore = new DirectoryServiceWaitOnSearch( 5000 ) )
        {
            restartServerWithOverriddenSettings( Map.of(
                    SecuritySettings.ldap_read_timeout, Duration.ofSeconds( 1 ),
                    SecuritySettings.ldap_authorization_connection_pooling, true,
                    SecuritySettings.ldap_authorization_use_system_account, true
            ) );

            assertReadFails( "neo", "abc123" );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldTimeoutIfLdapServerDoesNotRespondWithoutConnectionPooling() throws IOException
    {
        try ( DirectoryServiceWaitOnSearch ignore = new DirectoryServiceWaitOnSearch( 5000 ) )
        {
            restartServerWithOverriddenSettings( Map.of(
                    // NOTE: Pooled connections from previous test runs will not be affected by this read timeout setting
                    SecuritySettings.ldap_read_timeout, Duration.ofSeconds( 1 ),
                    SecuritySettings.ldap_authorization_connection_pooling, false,
                    SecuritySettings.ldap_authorization_use_system_account, true
            ) );

            assertReadFails( "neo", "abc123" );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldFailIfLdapSearchFails() throws IOException
    {
        try ( DirectoryServiceFailOnSearch ignore = new DirectoryServiceFailOnSearch() )
        {
            restartServerWithOverriddenSettings( Map.of(
                    SecuritySettings.ldap_read_timeout, Duration.ofSeconds( 1 ),
                    SecuritySettings.ldap_authorization_use_system_account, true
            ) );

            assertReadFails( "neo", "abc123" );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldTimeoutIfLdapServerDoesNotRespondWithLdapUserContext() throws IOException
    {
        try ( DirectoryServiceWaitOnSearch ignore = new DirectoryServiceWaitOnSearch( 5000 ) )
        {
            // When
            restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_read_timeout, Duration.ofSeconds( 1 ) ) );

            try
            {
                connectDriver( "neo", "abc123" );
                fail( "should have timed out" );
            }
            catch ( TransientException e )
            {
                assertThat( e.getMessage(), equalTo( LdapRealm.LDAP_READ_TIMEOUT_CLIENT_MESSAGE ) );
            }
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldGetCombinedAuthorization() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(
                SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME ),
                SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME ),
                SecuritySettings.ldap_authorization_use_system_account, true
        ) );

        // Given
        // we have a native 'tank' that is read only, and ldap 'tank' that is publisher
        createNativeUser( "tank", "localpassword", PredefinedRoles.READER );

        // Then
        // the created "tank" can log in and gets roles from both providers
        // because the system account is used to authorize over the ldap provider
        try ( Driver driver = connectDriver( "tank", "localpassword", "native" ) )
        {
            assertRoles( driver, PredefinedRoles.READER, PredefinedRoles.PUBLISHER );
        }

        // the ldap "tank" can also log in and gets roles from both providers
        try ( Driver driver = connectDriver( "tank", "abc123", "ldap" ) )
        {
            assertRoles( driver, PredefinedRoles.READER, PredefinedRoles.PUBLISHER );
        }
    }

    // ===== Logging tests =====

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldNotLogErrorsFromLdapRealmWhenLoginSuccessfulInNativeRealmNativeFirst() throws IOException, InvalidArgumentsException
    {
        restartServerWithOverriddenSettings( Map.of(
                SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME,SecuritySettings.LDAP_REALM_NAME ),
                SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME,SecuritySettings.LDAP_REALM_NAME ),
                SecuritySettings.ldap_authorization_use_system_account, true )
        );

        // Given
        // we have a native 'foo' that does not exist in ldap
        createNativeUser( "foo", "bar" );

        // Then
        // the created "foo" can log in
        assertAuth( "foo", "bar" );

        // We should not get errors spammed in the security log
        assertSecurityLogDoesNotContain( "ERROR" );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldNotLogErrorsFromLdapRealmWhenLoginSuccessfulInNativeRealmLdapFirst() throws IOException, InvalidArgumentsException
    {
        restartServerWithOverriddenSettings( Map.of(
                SecuritySettings.authentication_providers, List.of( SecuritySettings.LDAP_REALM_NAME,SecuritySettings.NATIVE_REALM_NAME ),
                SecuritySettings.authorization_providers, List.of( SecuritySettings.LDAP_REALM_NAME,SecuritySettings.NATIVE_REALM_NAME ),
                SecuritySettings.ldap_authorization_use_system_account, true )
        );

        // Given
        // we have a native 'foo' that does not exist in ldap
        createNativeUser( "foo", "bar" );

        // Then
        // the created "foo" can log in
        assertAuth( "foo", "bar" );

        // We should not get errors spammed in the security log
        assertSecurityLogDoesNotContain( "ERROR" );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldLogInvalidCredentialErrorFromLdapRealm() throws Throwable
    {
        // When
        assertAuthFail( "neo", "wrong-password" );

        // Then
        assertSecurityLogContains( LDAP_ERROR_MESSAGE_INVALID_CREDENTIALS );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldLogInvalidCredentialErrorFromLdapRealmWhenAllProvidersFail() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(
                SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME ),
                SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME ),
                SecuritySettings.ldap_authorization_use_system_account, true )
        );

        // Given
        // we have a native 'foo' that does not exist in ldap
        createNativeUser( "foo", "bar" );

        // When
        assertAuthFail( "foo", "wrong-password" );

        // Then
        assertSecurityLogContains( LDAP_ERROR_MESSAGE_INVALID_CREDENTIALS );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldLogConnectionRefusedFromLdapRealm() throws Throwable
    {
        // When
        restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_server, "ldap://" + REFUSED_IP ) );

        try
        {
            connectDriver( "neo", "abc123" );
            fail( "Expected connection refused" );
        }
        catch ( TransientException e )
        {
            assertThat( e.getMessage(), equalTo( LdapRealm.LDAP_CONNECTION_REFUSED_CLIENT_MESSAGE ) );
        }

        assertSecurityLogContains( "ERROR" );
        assertSecurityLogContains( "auth server connection refused" );
        assertSecurityLogContains( REFUSED_IP );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldLogConnectionRefusedFromLdapRealmWithMultipleRealms() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(
            SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME ),
            SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME ),
            SecuritySettings.ldap_authorization_use_system_account, true,
            SecuritySettings.ldap_server, "ldap://" + REFUSED_IP )
        );

        assertAuthFail( "neo", "abc123" );

        assertSecurityLogContains( "ERROR" );
        assertSecurityLogContains( "LDAP connection refused" );
        assertSecurityLogContains( REFUSED_IP );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldClearAuthenticationCache() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try ( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_server, "ldaps://localhost:" + sslLdapPort ) );

            // Then
            assertAuth( "tank", "abc123" );
            changeLDAPPassword( "tank", "abc123", "123abc" );

            // When logging in without clearing cache

            // Then
            assertAuthFail( "tank", "123abc" );
            assertAuth( "tank", "abc123" );

            // When clearing cache and logging in
            clearAuthCacheFromDifferentConnection();

            // Then
            assertAuthFail( "tank", "abc123" );
            assertAuth( "tank", "123abc" );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldClearAuthorizationCache() throws Throwable
    {
        getLdapServer().setConfidentialityRequired( true );

        try ( EmbeddedTestCertificates ignore = new EmbeddedTestCertificates() )
        {
            // When
            restartServerWithOverriddenSettings( Map.of( SecuritySettings.ldap_server, "ldaps://localhost:" + sslLdapPort ) );

            // Then
            try ( Driver driver = connectDriver( "tank", "abc123" ) )
            {
                assertReadSucceeds( driver );
                assertWriteSucceeds( driver );
            }

            changeLDAPGroup( "tank", "abc123", "reader" );

            // When logging in without clearing cache
            try ( Driver driver = connectDriver( "tank", "abc123" ) )
            {
                // Then
                assertReadSucceeds( driver );
                assertWriteSucceeds( driver );
            }

            // When clearing cache and logging in
            clearAuthCacheFromDifferentConnection();

            // Then
            try ( Driver driver = connectDriver( "tank", "abc123" ) )
            {
                assertReadSucceeds( driver );
                assertWriteFails( driver );
            }
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldNotSeeSystemPassword()
    {
        String ldapSettingName = SecuritySettings.ldap_authorization_system_password.name();
        String value = new SecureString( "" ).toString();
        String expected = String.format( "%s=%s", ldapSettingName, value );

        Config config = dbRule.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( Config.class );
        assertThat( "Should see obfuscated password in config.toString", config.toString(), containsString( expected ) );
        String password = config.get( SecuritySettings.ldap_authorization_system_password ).getString();
        assertThat( "Normal access should not be obfuscated", password, not( containsString( value ) ) );

        Logger log = mock( Logger.class );
        new ConfigDiagnostics( config ).dump( log );
        verify( log, atLeastOnce() ).log( "%s=%s", "dbms.security.ldap.authorization.system_password", value );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void shouldBeAbleToLoginAndAuthorizeWithLdapGroupHasUsersAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( Map.of(
                SecuritySettings.authentication_providers, List.of( SecuritySettings.PLUGIN_REALM_NAME_PREFIX + new LdapGroupHasUsersAuthPlugin().name() ),
                SecuritySettings.authorization_providers, List.of( SecuritySettings.PLUGIN_REALM_NAME_PREFIX + new LdapGroupHasUsersAuthPlugin().name() ) ) );

        Map<String,Object> parameters = MapUtil.map( "port", ldapServer.getPort() );

        try ( Driver driver = connectDriverWithParameters( "neo", "abc123", parameters ) )
        {
            assertRoles( driver, PredefinedRoles.READER );
        }

        try ( Driver driver = connectDriverWithParameters( "tank", "abc123", parameters ) )
        {
            assertRoles( driver, PredefinedRoles.PUBLISHER );
        }

        try ( Driver driver = connectDriverWithParameters( "smith", "abc123", parameters ) )
        {
            assertRoles( driver );
        }
    }

    // ===== Helpers =====

    private void modifyLDAPAttribute( String username, Object credentials, String attribute, Object value )
            throws Throwable
    {
        String principal = String.format( "cn=%s,ou=users,dc=example,dc=com", username );
        String principal1 = String.format( "cn=%s,ou=users,dc=example,dc=com", username );
        JndiLdapContextFactory contextFactory = new JndiLdapContextFactory();
        contextFactory.setUrl( "ldaps://localhost:" + sslLdapPort );
        LdapContext ctx = contextFactory.getLdapContext( principal1, credentials );

        ModificationItem[] mods = new ModificationItem[1];
        mods[0] = new ModificationItem( DirContext.REPLACE_ATTRIBUTE, new BasicAttribute( attribute, value ) );

        // Perform the update
        ctx.modifyAttributes( principal, mods );
        ctx.close();
    }

    @SuppressWarnings( "SameParameterValue" )
    private void changeLDAPPassword( String username, Object credentials, Object newCredentials ) throws Throwable
    {
        modifyLDAPAttribute( username, credentials, "userpassword", newCredentials );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void changeLDAPGroup( String username, Object credentials, String group ) throws Throwable
    {
        String gid;
        switch ( group )
        {
        case "reader":
            gid = "500";
            break;
        case "publisher":
            gid = "501";
            break;
        case "architect":
            gid = "502";
            break;
        case "admin":
            gid = "503";
            break;
        case "none":
            gid = "504";
            break;
        default:
            throw new IllegalArgumentException( "Invalid group name '" + group +
                                                "', expected one of none, reader, publisher, architect, or admin" );
        }
        modifyLDAPAttribute( username, credentials, "gidnumber", gid );
    }

    private class DirectoryServiceWaitOnSearch implements AutoCloseable
    {
        private final Interceptor waitOnSearchInterceptor;

        DirectoryServiceWaitOnSearch( long waitingTimeMillis )
        {
            waitOnSearchInterceptor = new BaseInterceptor()
            {
                @Override
                public String getName()
                {
                    return getClass().getName();
                }

                @Override
                public EntryFilteringCursor search( SearchOperationContext searchContext ) throws LdapException
                {
                    LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( waitingTimeMillis ) );
                    return super.search( searchContext );
                }
            };

            try
            {
                getService().addFirst( waitOnSearchInterceptor );
            }
            catch ( LdapException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void close()
        {
            getService().remove( waitOnSearchInterceptor.getName() );
        }
    }

    private class DirectoryServiceFailOnSearch implements AutoCloseable
    {
        private final Interceptor failOnSearchInterceptor;

        DirectoryServiceFailOnSearch()
        {
            failOnSearchInterceptor = new BaseInterceptor()
            {
                @Override
                public String getName()
                {
                    return getClass().getName();
                }

                @Override
                public EntryFilteringCursor search( SearchOperationContext searchContext ) throws LdapException
                {
                    throw new LdapOperationErrorException();
                }
            };

            try
            {
                getService().addFirst( failOnSearchInterceptor );
            }
            catch ( LdapException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void close()
        {
            getService().remove( failOnSearchInterceptor.getName() );
        }
    }
}
