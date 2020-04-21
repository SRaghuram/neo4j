/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.fail;

class DriverAuthHelper
{
    private static final Config config = Config.builder()
            .withLogging( Logging.none() )
            .withoutEncryption()
            .withConnectionTimeout( 10, TimeUnit.SECONDS )
            .build();

    static String boltUri( ConnectorPortRegister portRegister )
    {
        var localAddress = portRegister.getLocalAddress( "bolt" );
        return "bolt://" + localAddress.toString();
    }

    static void assertAuth( String uri, String username, String password )
    {
        assertAuth( uri, username, password, null );
    }

    static void assertAuth( String uri, String username, String password, String realm )
    {
        try ( Driver driver = connectDriver( uri, username, password, realm ) )
        {
            driver.verifyConnectivity();
        }
    }

    static void assertAuth( String uri, AuthToken authToken )
    {
        try ( Driver driver = connectDriver( uri, authToken ) )
        {
            driver.verifyConnectivity();
        }
    }

    static void assertAuthFail( String uri, String username, String password )
    {
        assertAuthFail( uri, username, password, null );
    }

    static void assertAuthFail( String uri, String username, String password, String realm )
    {
        try ( Driver ignored = connectDriver( uri, username, password, realm ) )
        {
            fail( "Should not have authenticated" );
        }
        catch ( AuthenticationException e )
        {
            assertThat( e.code(), CoreMatchers.equalTo( "Neo.ClientError.Security.Unauthorized" ) );
        }
    }

    static void assertReadSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            assertThat( single.asLong(), Matchers.greaterThanOrEqualTo( 1L ) );
        }
    }

    static void assertEmptyRead( String uri, String username, String password )
    {
        try ( Driver driver = connectDriver( uri, username, password ) )
        {
            assertEmptyRead( driver );
        }
    }

    static void assertEmptyRead( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 0L ) );
        }
    }

    static void assertWriteSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Result result = session.run( "CREATE ()" );
            assertThat( result.consume().counters().nodesCreated(), CoreMatchers.equalTo( 1 ) );
        }
    }

    static void assertWriteFails( Driver driver )
    {
        assertWriteFails( driver, "Create node with labels '' is not allowed for user " );
    }

    static void assertWriteFails( Driver driver, String errorMessage )
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE ()" ).consume();
            fail( "Should not be allowed write operation" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( errorMessage ) );
        }
    }

    static void assertProcSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "CALL test.staticReadProcedure()" ).single().get( 0 );
            assertThat( single.asString(), CoreMatchers.equalTo( "static" ) );
        }
    }

    static void assertAuthorizationExpired( Driver driver, String pluginName )
    {
        try ( Session session = driver.session() )
        {
            session.run( "MATCH (n) RETURN n" ).single();
            fail( "should have gotten authorization expired exception" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "Plugin '" + pluginName + "' authorization info expired" ) );
        }
    }

    static void clearAuthCacheFromDifferentConnection( String uri )
    {
        clearAuthCacheFromDifferentConnection( uri, "neo4j", "abc123", null );
    }

    static void clearAuthCacheFromDifferentConnection( String uri, String username, String password, String realm )
    {
        try ( Driver driver = connectDriver( uri, username, password, realm );
                Session session = driver.session() )
        {
            session.run( "CALL dbms.security.clearAuthCache()" );
        }
    }

    static Driver connectDriver( String uri, String username, String password )
    {
        return connectDriver( uri, username, password, null );
    }

    static Driver connectDriver( String uri, String username, String password, String realm )
    {
        AuthToken token;
        if ( realm == null || realm.isEmpty() )
        {
            token = AuthTokens.basic( username, password );
        }
        else
        {
            token = AuthTokens.basic( username, password, realm );
        }
        return connectDriver( uri, token, username, password );
    }

    static Driver connectDriverWithParameters( String uri, String username, String password, Map<String,Object> parameterMap )
    {
        AuthToken token = AuthTokens.custom( username, password, null, "basic", parameterMap );
        return connectDriver( uri, token, username, password );
    }

    static Driver connectDriver( String uri, AuthToken token )
    {
        return connectDriver( uri, token, null, null );
    }

    private static Driver connectDriver( String uri, AuthToken token, String username, String password )
    {
        try
        {
            Driver driver = GraphDatabase.driver( uri, token, config );
            driver.verifyConnectivity();
            return driver;
        }
        catch ( AuthenticationException e )
        {
            if ( username != null && password != null )
            {
                throw new FullCredentialsAuthenticationException( e, username, password );
            }
            throw e;
        }
    }

    static void assertRoles( Driver driver, String... roles )
    {
        try ( Session session = driver.session() )
        {
            Record record = session.run( "CALL dbms.showCurrentUser() YIELD roles" ).single();
            assertThat( record.get( "roles" ).asList(), containsInAnyOrder( roles ) );
        }
    }
}
