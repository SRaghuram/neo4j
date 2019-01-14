/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Test;

import java.util.Map;

import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.password;
import static org.neo4j.test.AuthTokenUtil.authTokenMatcher;

public class ShiroAuthTokenTest
{
    private static final String USERNAME = "myuser";
    private static final String PASSWORD = "mypw123";

    @Test
    public void shouldSupportBasicAuthToken() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    public void shouldSupportBasicAuthTokenWithEmptyRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, "" ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "" ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    public void shouldSupportBasicAuthTokenWithNullRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, null ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, null ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    public void shouldSupportBasicAuthTokenWithWildcardRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, "*" ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "*" ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    public void shouldSupportBasicAuthTokenWithSpecificRealm() throws Exception
    {
        String realm = "ldap";
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, realm ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap" ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    public void shouldSupportCustomAuthTokenWithSpecificRealm() throws Exception
    {
        String realm = "ldap";
        ShiroAuthToken token =
                new ShiroAuthToken( AuthToken.newCustomAuthToken( USERNAME, PASSWORD, realm, AuthToken.BASIC_SCHEME ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap" ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    public void shouldSupportCustomAuthTokenWithSpecificRealmAndParameters() throws Exception
    {
        String realm = "ldap";
        Map<String,Object> params = map( "a", "A", "b", "B" );
        ShiroAuthToken token =
                new ShiroAuthToken(
                        AuthToken.newCustomAuthToken( USERNAME, PASSWORD, realm, AuthToken.BASIC_SCHEME, params ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap",
                        "parameters", params ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    public void shouldHaveStringRepresentationWithNullRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, null ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );

        String stringRepresentation = token.toString();
        assertThat( stringRepresentation, containsString( "realm='null'" ) );
    }

    private void testTokenSupportsRealm( ShiroAuthToken token, boolean supports, String... realms )
    {
        for ( String realm : realms )
        {
            assertThat( "Token should support '" + realm + "' realm", token.supportsRealm( realm ),
                    equalTo( supports ) );
        }
    }

    private void testBasicAuthToken( ShiroAuthToken token, String username, String password, String scheme )
            throws InvalidAuthTokenException
    {
        assertThat( "Token should have basic scheme", token.getScheme(), equalTo( scheme ) );
        assertThat( "Token have correct principal", token.getPrincipal(), equalTo( username ) );
        assertThat( "Token have correct credentials", token.getCredentials(), equalTo( password( password ) ) );
    }
}
