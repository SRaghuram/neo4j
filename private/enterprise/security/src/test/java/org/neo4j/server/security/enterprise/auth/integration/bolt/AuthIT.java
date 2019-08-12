/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.junit.Assume.assumeTrue;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuthFail;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertProcSucceeds;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertReadFails;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertReadSucceeds;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertWriteFails;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertWriteSucceeds;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.connectDriver;

@SuppressWarnings( "deprecation" )
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
public class AuthIT extends EnterpriseLdapAuthenticationTestBase
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
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldaps", "abc123", true, true, "localhost",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"StartTLS", "abc123", true, false, "localhost",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "true",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"LdapSystemAccount", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldaps SystemAccount", "abc123", true, true, "localhost",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"StartTLS SystemAccount", "abc123", true, false, "localhost",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "true",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldap authn cache disabled", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_cache_enabled, "false"
                        )
                },
                {"Ldap Digest MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_mechanism, "DIGEST-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap Cram MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_mechanism, "CRAM-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap authn Native authz", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "false",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap authz Native authn", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "false",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldap with Native authn", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap with Native authz", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap and Native", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap with AD", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=local,ou=users,dc=example,dc=com",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(samaccountname={0}))",
                                SecuritySettings.ldap_authorization_group_membership_attribute_names, "memberOf",
                                SecuritySettings.ldap_authorization_group_to_role_mapping,
                                "cn=reader,ou=groups,dc=example,dc=com=reader;" +
                                        "cn=publisher,ou=groups,dc=example,dc=com=publisher;" +
                                        "cn=architect,ou=groups,dc=example,dc=com=architect;" +
                                        "cn=admin,ou=groups,dc=example,dc=com=admin;" +
                                        "cn=role1,ou=groups,dc=example,dc=com=role1",
                                SecuritySettings.ldap_authentication_use_samaccountname, "true"
                        )
                },
                {"Native with unresponsive ldap", "abc123", false, false, "127.0.0.1",
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Native", "abc123", false, false, "0.0.0.0",
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true"
                        )
                }
        } );
    }

    private static final String NONE_USER = "smith";
    private static final String READ_USER = "neo";
    private static final String WRITE_USER = "tank";
    private static final String PROC_USER = "jane";

    private final String password;
    private final Map<Setting<?>,String> configMap;
    private final boolean confidentialityRequired;
    private final boolean ldapWithAD;

    @SuppressWarnings( "unused" )
    public AuthIT( String suiteName, String password, boolean confidentialityRequired, boolean secureLdap, String host, List<Object> settings )
    {
        this.password = password;
        this.confidentialityRequired = confidentialityRequired;
        this.configMap = new HashMap<>();
        configMap.put( SecuritySettings.ldap_server, getLdapServerUri( secureLdap, host ) );
        for ( int i = 0; i < settings.size() - 1; i += 2 )
        {
            Setting setting = (Setting) settings.get( i );
            String value = (String) settings.get( i + 1 );
            configMap.put( setting, value );
        }

        ldapWithAD = suiteName.equals( "Ldap with AD" );
    }

    @BeforeClass
    public static void classSetup()
    {
        boolean isWindows = System.getProperty( "os.name" ).toLowerCase().startsWith( "windows" );
        Assume.assumeFalse( isWindows );
        embeddedTestCertificates = new EmbeddedTestCertificates();
    }

    @Before
    public void setup() throws Exception
    {
        startDatabase();
        LdapServer ldapServer = ldapServerRule.getLdapServer();
        ldapServer.setConfidentialityRequired( confidentialityRequired );

        EnterpriseAuthAndUserManager authManager = dbRule.resolveDependency( EnterpriseAuthAndUserManager.class );
        EnterpriseUserManager userManager = authManager.getUserManager();
        if ( userManager != null )
        {
            userManager.newUser( NONE_USER, password.getBytes(), false );
            userManager.newUser( PROC_USER, password.getBytes(), false );
            userManager.newUser( READ_USER, password.getBytes(), false );
            userManager.newUser( WRITE_USER, password.getBytes(), false );
            userManager.addRoleToUser( PredefinedRoles.READER, READ_USER );
            userManager.addRoleToUser( PredefinedRoles.PUBLISHER, WRITE_USER );
            userManager.newRole( "role1", PROC_USER );
        }
        checkIfLdapServerIsReachable( ldapServer.getSaslHost(), ldapServer.getPort() );
    }

    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        Map<Setting<?>, String> settings = new HashMap<>();

        settings.put( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
        settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "gidnumber" );
        settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin;505=role1" );
        settings.put( SecuritySettings.ldap_authentication_cache_enabled, "true" );
        settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
        settings.put( SecuritySettings.procedure_roles, "test.staticReadProcedure:role1" );
        settings.put( SecuritySettings.ldap_read_timeout, "1s" );
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
        assertAuth( boltUri, READ_USER, password );
        assertAuth( boltUri, READ_USER, password );
    }

    @Test
    public void shouldFailLoginWithIncorrectCredentials()
    {
        assertAuthFail( boltUri, READ_USER, "WRONG" );
        assertAuthFail( boltUri, READ_USER, "ALSO WRONG" );
    }

    @Test
    public void shouldFailLoginWithInvalidCredentialsFollowingSuccessfulLogin()
    {
        assertAuth( boltUri, READ_USER, password );
        assertAuthFail( boltUri, READ_USER, "WRONG" );
    }

    @Test
    public void shouldLoginFollowingFailedLogin()
    {
        assertAuthFail( boltUri, READ_USER, "WRONG" );
        assertAuth( boltUri, READ_USER, password );
    }

    @Test
    public void shouldGetCorrectAuthorization()
    {
        try ( Driver driver = connectDriver( boltUri, NONE_USER, password ) )
        {
            assertReadFails( driver );
            assertWriteFails( driver );
        }
        try ( Driver driver = connectDriver( boltUri, READ_USER, password ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
        try ( Driver driver = connectDriver( boltUri, WRITE_USER, password ) )
        {
            assertReadSucceeds( driver );
            assertWriteSucceeds( driver );
        }
        try ( Driver driver = connectDriver( boltUri, PROC_USER, password ) )
        {
            assertProcSucceeds( driver );
            assertReadFails( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldLoginWithSamAccountName()
    {
        assumeTrue( ldapWithAD );

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
        assertAuthFail( boltUri, "luser", "wrong" );
    }

    @Test
    public void shouldFailLoginSamAccountNameWithDN()
    {
        assumeTrue( ldapWithAD );
        assertAuthFail( boltUri, "local.user", "abc123" );
    }

    @Test
    public void shouldReadWithSamAccountName()
    {
        assumeTrue( ldapWithAD );

        try ( Driver driver = connectDriver( boltUri, "luser", "abc123" ) )
        {
            assertReadSucceeds( driver );
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
