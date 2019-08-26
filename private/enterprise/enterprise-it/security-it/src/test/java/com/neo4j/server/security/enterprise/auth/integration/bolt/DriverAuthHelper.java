/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.test.rule.DbmsRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.fail;

public class DriverAuthHelper
{
    public static final Config config = Config.build()
            .withLogging( Logging.none() )
            .withoutEncryption()
            .withConnectionTimeout( 10, TimeUnit.SECONDS )
            .build();

    public static String boltUri( DbmsRule dbmsRule )
    {
        var localAddress = dbmsRule.resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        return "bolt://" + localAddress.toString();
    }

    public static void assertAuth( String uri, String username, String password )
    {
        assertAuth( uri, username, password, null );
    }

    public static void assertAuth( String uri, String username, String password, String realm )
    {
        try ( Driver driver = connectDriver( uri, username, password, realm );
                Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), CoreMatchers.equalTo( 1L ) );
        }
    }

    public static void assertAuth( String uri, AuthToken authToken )
    {
        try ( Driver driver = connectDriver( uri, authToken );
                Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), CoreMatchers.equalTo( 1L ) );
        }
    }

    public static void assertAuthFail( String uri, String username, String password )
    {
        assertAuthFail( uri, username, password, null );
    }

    public static void assertAuthFail( String uri, String username, String password, String realm )
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

    public static void assertReadSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            assertThat( single.asLong(), Matchers.greaterThanOrEqualTo( 0L ) );
        }
    }

    public static void assertReadFails( String uri, String username, String password )
    {
        try ( Driver driver = connectDriver( uri, username, password ) )
        {
            assertReadFails( driver );
        }
    }

    public static void assertReadFails( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            session.run( "MATCH (n) RETURN count(n)" ).single().get( 0 );
            fail( "Should not be allowed read operation" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "Read operations are not allowed for user " ) );
        }
    }

    public static void assertWriteSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            StatementResult result = session.run( "CREATE ()" );
            assertThat( result.summary().counters().nodesCreated(), CoreMatchers.equalTo( 1 ) );
        }
    }

    public static void assertWriteFails( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE ()" ).consume();
            fail( "Should not be allowed write operation" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "Write operations are not allowed for user " ) );
        }
    }

    public static void assertProcSucceeds( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Value single = session.run( "CALL test.staticReadProcedure()" ).single().get( 0 );
            assertThat( single.asString(), CoreMatchers.equalTo( "static" ) );
        }
    }

    public static void assertAuthorizationExpired( Driver driver, String pluginName )
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

    public static void clearAuthCacheFromDifferentConnection( String uri )
    {
        clearAuthCacheFromDifferentConnection( uri, "neo4j", "abc123", null );
    }

    public static void clearAuthCacheFromDifferentConnection( String uri, String username, String password, String realm )
    {
        try ( Driver driver = connectDriver( uri, username, password, realm );
                Session session = driver.session() )
        {
            session.run( "CALL dbms.security.clearAuthCache()" );
        }
    }

    public static Driver connectDriver( String uri, String username, String password )
    {
        return connectDriver( uri, username, password, null );
    }

    public static Driver connectDriver( String uri, String username, String password, String realm )
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

    public static Driver connectDriverWithParameters( String uri, String username, String password, Map<String,Object> parameterMap )
    {
        AuthToken token = AuthTokens.custom( username, password, null, "basic", parameterMap );
        return connectDriver( uri, token, username, password );
    }

    private static Driver connectDriver( String uri, AuthToken token )
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
