/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.string.SecureString;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

/*
 * Only run these tests when the appropriate ActiveDirectory server is in fact live.
 * The tests remain here because they are very useful when developing and testing Active Directory
 * security features. Regular automated testing of Active Directory security should also be handled
 * in the smoke tests run downstream of the main build, so the fact that these tests are not run during
 * the main build should not be of serious concern.
 *
 * Note also that most of the security code related to Active Directory is identical to the LDAP code,
 * and so the tests in LdapAuthIT, which are run during normal build, do in fact test that
 * code. Testing against a real Active Directory is not possible during a build phase, and therefor
 * we keep this disabled by default.
 */
@Ignore
public class ActiveDirectoryAuthenticationIT
{
    @Rule
    public Neo4jWithSocket server =
            new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), asSettings( getSettingsFunction() ) );

    private void restartNeo4jServerWithOverriddenSettings( Consumer<Map<Setting<?>,Object>> overrideSettingsFunction )
    {
        server.shutdownManagementService();
        server.ensureDatabase( asSettings( overrideSettingsFunction ) );
    }

    private Consumer<Map<Setting<?>,Object>> asSettings( Consumer<Map<Setting<?>,Object>> overrideSettingsFunction )
    {
        return settings ->
        {
            Map<Setting<?>,Object> o = new LinkedHashMap<>();
            overrideSettingsFunction.accept( o );
            for ( Setting key : o.keySet() )
            {
                settings.put( key, o.get( key ) );
            }
        };
    }

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder();
    }

    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled, true );
            settings.put( SecuritySettings.authentication_providers, List.of( SecuritySettings.LDAP_REALM_NAME ) );
            settings.put( SecuritySettings.authorization_providers, List.of( SecuritySettings.LDAP_REALM_NAME ) );
            settings.put( SecuritySettings.ldap_server, "activedirectory.neohq.net" );
            settings.put( SecuritySettings.ldap_authentication_user_dn_template, "CN={0},CN=Users,DC=neo4j,DC=com" );
            settings.put( SecuritySettings.ldap_authorization_use_system_account, false );
            settings.put( SecuritySettings.ldap_authorization_user_search_base, "cn=Users,dc=neo4j,dc=com" );
            settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(CN={0}))" );
            settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, List.of( "memberOf" ) );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping,
                    "'CN=Neo4j Read Only,CN=Users,DC=neo4j,DC=com'=reader;" +
                    "CN=Neo4j Read-Write,CN=Users,DC=neo4j,DC=com=publisher;" +
                    "CN=Neo4j Schema Manager,CN=Users,DC=neo4j,DC=com=architect;" +
                    "CN=Neo4j Administrator,CN=Users,DC=neo4j,DC=com=admin" );
        };
    }

    private Consumer<Map<Setting<?>,Object>> useSystemAccountSettings = settings ->
    {
        settings.put( SecuritySettings.ldap_authorization_use_system_account, true );
        settings.put( SecuritySettings.ldap_authorization_system_username, "Neo4j System" );
        settings.put( SecuritySettings.ldap_authorization_system_password, new SecureString( "ProudListingsMedia1" ) );
    };

    public Factory<TransportConnection> cf = SecureSocketConnection::new;

    private HostnamePort address;
    private TransportConnection client;
    private TransportTestUtil util;

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
        this.address = server.lookupDefaultConnector();
        this.util = new TransportTestUtil();
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    //------------------------------------------------------------------
    // Active Directory tests on EC2
    // NOTE: These rely on an external server and are not executed by automated testing
    //       They are here as a convenience for running local testing.

    @Test
    public void shouldNotBeAbleToLoginUnknownUserOnEC2() throws Throwable
    {

        assertAuthFail( "unknown", "ProudListingsMedia1" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextOnEC2() throws Throwable
    {
        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherWithUserLdapContextOnEC2() throws Throwable
    {
        assertAuth( "tank", "ProudListingsMedia1" );
        assertWriteSucceeds();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizePublisherOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings );

        assertAuth( "tank", "ProudListingsMedia1" );
        assertWriteSucceeds();
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserWithUserLdapContextOnEC2() throws Throwable
    {
        assertAuth( "smith", "ProudListingsMedia1" );
        assertReadFails( "'smith' with no roles" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeNoPermissionUserOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings );

        assertAuth( "smith", "ProudListingsMedia1" );
        assertReadFails( "'smith' with no roles" );
    }

    //------------------------------------------------------------------
    // Secure Active Directory tests on EC2
    // NOTE: These tests does not work together with EmbeddedTestCertificates used in the embedded secure LDAP tests!
    //       (This is because the embedded tests override the Java default key/trust store locations using
    //        system properties that will not be re-read)

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderUsingLdapsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings
                .andThen( settings -> settings.put( SecuritySettings.ldap_server, "ldaps://activedirectory.neohq.net:636" ) ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingLDAPSOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings(
                settings -> settings.put( SecuritySettings.ldap_server, "ldaps://activedirectory.neohq.net:636" ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderUsingStartTlsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( useSystemAccountSettings
                .andThen( settings -> settings.put( SecuritySettings.ldap_use_starttls, true ) ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeReaderWithUserLdapContextUsingStartTlsOnEC2() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.ldap_use_starttls, true ) );

        assertAuth( "neo", "ProudListingsMedia1" );
        assertReadSucceeds();
        assertWriteFails( "'neo' with roles [reader]" );
    }

    @Test
    public void shouldBeAbleToAccessEC2ActiveDirectoryInstance() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings ->
        {
        } );

        // When
        assertAuth( "tank", "ProudListingsMedia1" );

        // Then
        assertReadSucceeds();
        assertWriteSucceeds();
    }

    private void assertAuth( String username, String password ) throws Exception
    {
        assertAuth( username, password, null );
    }

    private void assertAuth( String username, String password, String realm ) throws Exception
    {
        client.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( authToken( username, password, realm ) ) );

        assertThat( client ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private Map<String,Object> authToken( String username, String password, String realm )
    {
        if ( realm != null && realm.length() > 0 )
        {
            return map( "principal", username, "credentials", password, "scheme", "basic", "realm", realm );
        }
        else
        {
            return map( "principal", username, "credentials", password, "scheme", "basic" );
        }
    }

    private void assertAuthFail( String username, String password ) throws Exception
    {
        client.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", username, "credentials", password, "scheme", "basic" ) ) );

        assertThat( client ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( client ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );
    }

    protected void assertReadSucceeds() throws Exception
    {
        // When
        client.send( util.defaultRunAutoCommitTx( "MATCH (n) RETURN n" ) );

        // Then
        assertThat( client ).satisfies( util.eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertReadFails( String username ) throws Exception
    {
        // When
        client.send( util.defaultRunAutoCommitTx( "MATCH (n) RETURN n" ) );

        // Then
        assertThat( client ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Read operations are not allowed for user %s.", username ) ) ) );
    }

    protected void assertWriteSucceeds() throws Exception
    {
        // When
        client.send( util.defaultRunAutoCommitTx( "CREATE ()" ) );

        // Then
        assertThat( client ).satisfies( util.eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertWriteFails( String username ) throws Exception
    {
        // When
        client.send( util.defaultRunAutoCommitTx( "CREATE ()" ) );

        // Then
        assertThat( client ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        String.format( "Create node with labels '' is not allowed for user %s.", username ) ) ) );
    }

}
