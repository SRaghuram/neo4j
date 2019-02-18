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

import java.time.Clock;
import java.util.Map;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.configuration.Config;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.password;
import static org.neo4j.test.assertion.Assert.assertException;

public class BoltInitChangePasswordTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule( new InMemoryUserRepository(),
            new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() ) );
    private BasicAuthentication authentication;

    @Before
    public void setup() throws Throwable
    {
        authentication = new BasicAuthentication( authManagerRule.getManager(), authManagerRule.getManager() );
        authManagerRule.getManager().getUserManager().newUser( "neo4j", password( "123" ), true );
    }

    @Test
    public void shouldLogInitPasswordChange() throws Throwable
    {
        authentication.authenticate( authToken( "neo4j", "123", "secret" ) );

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "logged in (password change required)" );
        fullLog.assertHasLine( "neo4j", "changed password" );
    }

    @Test
    public void shouldLogFailedInitPasswordChange()
    {
        assertException( () -> authentication.authenticate( authToken( "neo4j", "123", "123" ) ),
                AuthenticationException.class, "Old password and new password cannot be the same." );

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "logged in (password change required)" );
        fullLog.assertHasLine( "neo4j", "tried to change password: Old password and new password cannot be the same." );
    }

    private Map<String,Object> authToken( String username, String password, String newPassword )
    {
        return map( "principal", username, "credentials", password( password ),
                "new_credentials", password( newPassword ), "scheme", "basic" );
    }
}
