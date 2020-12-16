/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuthFail;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertEmptyRead;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertProcSucceeds;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertReadSucceeds;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertWriteFails;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertWriteSucceeds;
import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.connectDriver;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.driver.SessionConfig.forDatabase;

@RunWith( Parameterized.class )
@CreateDS(
        name = "TestAuthIT",
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
public class AuthIT extends EnterpriseLdapAuthTestBase
{
    private static EmbeddedTestCertificates embeddedTestCertificates;

    @ClassRule
    public static final CreateLdapServerRule ldapServerRule = new CreateLdapServerRule();

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> configurations()
    {
        return Arrays.asList( new Object[][]{
                {"Ldap", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"Ldaps", "abc123", true, true, "localhost",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"StartTLS", "abc123", true, false, "localhost",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, TRUE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"LdapSystemAccount", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, TRUE,
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldaps SystemAccount", "abc123", true, true, "localhost",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, TRUE,
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"StartTLS SystemAccount", "abc123", true, false, "localhost",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, TRUE,
                                SecuritySettings.ldap_authorization_use_system_account, TRUE,
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldap authn cache disabled", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE,
                                SecuritySettings.ldap_authentication_cache_enabled, FALSE
                        )
                },
                {"Ldap Digest MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE,
                                SecuritySettings.ldap_authentication_mechanism, "DIGEST-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap Cram MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE,
                                SecuritySettings.ldap_authentication_mechanism, "CRAM-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap authn Native authz", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"Ldap authz Native authn", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, TRUE,
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldap with Native authn", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"Ldap with Native authz", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"Ldap and Native", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"Ldap with AD", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=local,ou=users,dc=example,dc=com",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_use_system_account, TRUE,
                                SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(samaccountname={0}))",
                                SecuritySettings.ldap_authorization_group_membership_attribute_names, "memberOf",
                                SecuritySettings.ldap_authorization_group_to_role_mapping,
                                "cn=reader,ou=groups,dc=example,dc=com=reader;" +
                                        "cn=publisher,ou=groups,dc=example,dc=com=publisher;" +
                                        "cn=architect,ou=groups,dc=example,dc=com=architect;" +
                                        "cn=admin,ou=groups,dc=example,dc=com=admin;" +
                                        "cn=role1,ou=groups,dc=example,dc=com=role1",
                                SecuritySettings.ldap_authentication_use_attribute, TRUE
                        )
                },
                {"Native with unresponsive ldap", "abc123", false, false, "127.0.0.1",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, FALSE,
                                SecuritySettings.ldap_authorization_use_system_account, FALSE
                        )
                },
                {"Native", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.authorization_providers, SecuritySettings.NATIVE_REALM_NAME
                        )
                }
        } );
    }

    private static final String NONE_USER = "smith";
    private static final String READ_USER = "neo";
    private static final String WRITE_USER = "tank";
    private static final String PROC_USER = "jane";
    private static final String ADMIN_USER = "neo4j";

    private final String password;
    private final Map<Setting<?>,Object> configMap;
    private final boolean confidentialityRequired;
    private final boolean ldapWithAD;
    private final boolean createUsers;

    @SuppressWarnings( {"unused", "unchecked"} )
    public AuthIT( String suiteName, String password, boolean confidentialityRequired, boolean secureLdap, String host, List<Object> settings )
    {
        this.password = password;
        this.confidentialityRequired = confidentialityRequired;
        this.configMap = new HashMap<>();
        configMap.put( SecuritySettings.ldap_server, getLdapServerUri( secureLdap, host ) );
        boolean nativeEnabled = false;
        for ( int i = 0; i < settings.size() - 1; i += 2 )
        {
            SettingImpl<Object> setting = (SettingImpl<Object>) settings.get( i );
            String value = (String) settings.get( i + 1 );
            if ( (setting.equals( SecuritySettings.authentication_providers ) || setting.equals( SecuritySettings.authorization_providers )) &&
                    value.contains( SecuritySettings.NATIVE_REALM_NAME ) )
            {
                nativeEnabled = true;
            }
            configMap.put( setting, setting.parse( value ) );
        }

        createUsers = nativeEnabled;
        ldapWithAD = suiteName.equals( "Ldap with AD" );
    }

    @BeforeClass
    public static void classSetup()
    {
        boolean isWindows = System.getProperty( "os.name" ).toLowerCase().startsWith( "windows" );
        Assume.assumeFalse( isWindows );
        embeddedTestCertificates = new EmbeddedTestCertificates();
    }

    public void setup()
    {
        startDatabase();
        LdapServer ldapServer = ldapServerRule.getLdapServer();
        ldapServer.setConfidentialityRequired( confidentialityRequired );

        createRole( "role1" );
        if ( createUsers )
        {
            executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", NONE_USER, password ) );
            executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", PROC_USER, password ) );
            executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", READ_USER, password ) );
            executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", WRITE_USER, password ) );
            executeOnSystem( String.format( "ALTER USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", ADMIN_USER, password ) );
            executeOnSystem( String.format( "GRANT ROLE %s TO %s", PredefinedRoles.READER, READ_USER ) );
            executeOnSystem( String.format( "GRANT ROLE %s TO %s", PredefinedRoles.PUBLISHER, WRITE_USER ) );
            executeOnSystem( String.format( "GRANT ROLE %s TO %s", "role1", PROC_USER ) );
        }
        checkIfLdapServerIsReachable( ldapServer.getSaslHost(), ldapServer.getPort() );
    }

    @Override
    protected Map<Setting<?>, Object> getSettings()
    {
        Map<Setting<?>, Object> settings = new HashMap<>();

        settings.put( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
        settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, List.of( "gidnumber" ) );
        settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin;505=role1" );
        settings.put( SecuritySettings.ldap_authentication_cache_enabled, true );
        settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
        settings.put( GraphDatabaseSettings.procedure_roles, "test.staticReadProcedure:role1" );
        settings.put( SecuritySettings.ldap_read_timeout, Duration.ofSeconds( 1 ) );
        settings.putAll( configMap );
        return settings;
    }

    @AfterClass
    public static void classTeardown()
    {
        if ( embeddedTestCertificates != null )
        {
            embeddedTestCertificates.close();
        }
    }

    @Test
    public void shouldLoginWithCorrectInformation()
    {
        setup();
        assertAuth( boltUri, READ_USER, password );
        assertAuth( boltUri, READ_USER, password );
    }

    @Test
    public void shouldFailLoginWithIncorrectCredentials()
    {
        setup();
        assertAuthFail( boltUri, READ_USER, "WRONG" );
        assertAuthFail( boltUri, READ_USER, "ALSO WRONG" );
    }

    @Test
    public void shouldFailLoginWithInvalidCredentialsFollowingSuccessfulLogin()
    {
        setup();
        assertAuth( boltUri, READ_USER, password );
        assertAuthFail( boltUri, READ_USER, "WRONG" );
    }

    @Test
    public void shouldLoginFollowingFailedLogin()
    {
        setup();
        assertAuthFail( boltUri, READ_USER, "WRONG" );
        assertAuth( boltUri, READ_USER, password );
    }

    @Test
    public void shouldGetCorrectAuthorizationNoExplicitRoles()
    {
        setup();
        try ( Driver driver = connectDriver( boltUri, NONE_USER, password ) )
        {
            assertEmptyRead( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldGetCorrectAuthorizationReaderUser()
    {
        setup();
        try ( Driver driver = connectDriver( boltUri, READ_USER, password ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldGetCorrectAuthorizationWriteUser()
    {
        setup();
        try ( Driver driver = connectDriver( boltUri, WRITE_USER, password ) )
        {
            assertReadSucceeds( driver );
            assertWriteSucceeds( driver );
        }
    }

    @Test
    public void shouldGetCorrectAuthorizationAllowedProcedure() throws KernelException
    {
        setup();
        dbRule.resolveDependency( GlobalProcedures.class ).registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );
        try ( Driver driver = connectDriver( boltUri, PROC_USER, password ) )
        {
            assertProcSucceeds( driver );
            assertEmptyRead( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldShowDatabasesOnSystem()
    {
        setup();
        try ( Driver driver = connectDriver( boltUri, READ_USER, password ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                List<Record> records = session.run( "SHOW DATABASES" ).list();
                assertThat( records.size(), equalTo( 2 ) );
            }
        }
    }

    @Test
    public void shouldLoginWithSamAccountName()
    {
        assumeTrue( ldapWithAD );
        setup();
        // dn: cn=local.user,ou=local,ou=users,dc=example,dc=com
        assertAuth( boltUri, "luser", "abc123" );
        assertAuth( boltUri, "luser", "abc123" );
        // dn: cn=remote.user,ou=remote,ou=users,dc=example,dc=com
        assertAuth( boltUri, "ruser", "abc123" );
        assertAuth( boltUri, "ruser", "abc123" );
    }

    @Test
    public void shouldFailLoginSamAccountNameWrongPassword()
    {
        assumeTrue( ldapWithAD );
        setup();
        assertAuthFail( boltUri, "luser", "wrong" );
    }

    @Test
    public void shouldFailLoginSamAccountNameWithDN()
    {
        assumeTrue( ldapWithAD );
        setup();
        assertAuthFail( boltUri, "local.user", "abc123" );
    }

    @Test
    public void shouldReadWithSamAccountName()
    {
        assumeTrue( ldapWithAD );
        setup();

        try ( Driver driver = connectDriver( boltUri, "luser", "abc123" ) )
        {
            assertReadSucceeds( driver );
        }
    }

    @Test
    public void shouldFailNicelyOnCreateDuplicateUser()
    {
        assumeTrue( createUsers );
        setup();

        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, password ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE USER " + READ_USER + " SET PASSWORD 'foo'" ).list();
                fail( "should have gotten exception" );
            }
            catch ( ClientException ce )
            {
                assertThat( ce.getMessage(), equalTo( "Failed to create the specified user '" + READ_USER + "': User already exists." ) );
            }
        }
    }

    @Test
    public void shouldFailNicelyOnAlterUserPassword()
    {
        assumeTrue( createUsers );
        setup();

        // GIVEN
        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, password );
              Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
        {
            session.run( "CREATE USER fooUser SET PASSWORD 'foo'" ).consume();
        }

        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, password );
              Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
        {
            // WHEN
            session.run( "ALTER USER fooUser SET PASSWORD 'foo' CHANGE NOT REQUIRED" ).consume();
            fail( "should have gotten exception" );
        }
        catch ( ClientException ce )
        {
            // THEN
            assertThat( ce.getMessage(), equalTo( "Failed to alter the specified user 'fooUser': Old password and new password cannot be the same." ) );
        }
    }

    @Test
    public void shouldFailNicelyOnAlterCurrentUserPassword()
    {
        assumeTrue( createUsers );
        setup();

        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, password ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "ALTER CURRENT USER SET PASSWORD FROM 'wrongCredentials' TO 'newPassword'" ).list();
                fail( "should have gotten exception" );
            }
            catch ( ClientException ce )
            {
                assertThat( ce.getMessage(),
                        equalTo( "User '" + ADMIN_USER + "' failed to alter their own password: Invalid principal or credentials." ) );
            }
        }
    }

    @Test
    public void shouldFailNicelyOnGrantToNonexistentRole()
    {
        assumeTrue( createUsers );
        setup();

        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, password ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "GRANT ROLE none TO " + READ_USER ).list();
                fail( "should have gotten exception" );
            }
            catch ( ClientException ce )
            {
                assertThat( ce.getMessage(), equalTo( "Failed to grant role 'none' to user '" + READ_USER + "': Role does not exist." ) );
            }
        }
    }

    private static String getLdapServerUri( boolean secureLdap, String host )
    {
        LdapServer ldapServer = ldapServerRule.getLdapServer();
        return secureLdap ? getLdapUri( "ldaps", host, ldapServer.getPortSSL() ) : getLdapUri( "ldap", host, ldapServer.getPort() );
    }

    private static String getLdapUri( String protocol, String host, int port )
    {
        return protocol + "://" + host + ":" + port;
    }
}
