/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.auth.MultiRealmAuthManagerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.BasicAuthentication;

import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.assertion.Assert.assertException;

public class BoltInitChangePasswordTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule();
    private BasicAuthentication authentication;

    @Before
    public void setup() throws Throwable
    {
        authentication = new BasicAuthentication( authManagerRule.getManager(), authManagerRule.getManager() );
        authManagerRule.getManager().getUserManager().newUser( "user", password( "123" ), true );
    }

    @Test
    public void shouldLogInitPasswordChange() throws Throwable
    {
        authentication.authenticate( authToken( "user", "123", "secret" ) );

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "user", "logged in (password change required)" );
        fullLog.assertHasLine( "user", "changed password" );
    }

    @Test
    public void shouldLogFailedInitPasswordChange()
    {
        assertException( () -> authentication.authenticate( authToken( "user", "123", "123" ) ),
                AuthenticationException.class, "Old password and new password cannot be the same." );

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "user", "logged in (password change required)" );
        fullLog.assertHasLine( "user", "tried to change password: Old password and new password cannot be the same." );
    }

    private Map<String,Object> authToken( String username, String password, String newPassword )
    {
        return map( "principal", username, "credentials", password( password ),
                "new_credentials", password( newPassword ), "scheme", "basic" );
    }
}
