/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

public class SecurityHelpers
{
    private SecurityHelpers()
    {
    }

    public static void newUser( Driver driver, String username, String password )
    {
        try ( Session session = driver.session( SessionConfig.forDatabase( "system" ) ) )
        {
            session.writeTransaction( tx -> tx.run( format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", username, password ) ) );
        }
    }

    public static void newUser( Transaction tx, String username, String password )
    {
        tx.execute( format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", username, password ) );
    }

    public static Result showUsers( Transaction tx )
    {
        return tx.execute( "SHOW USERS" );
    }

    public static Set<String> getAllRoleNames( DatabaseManagementService dbms )
    {
        try ( Transaction tx = dbms.database( SYSTEM_DATABASE_NAME ).beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            var roles = new HashSet<String>();
            result.forEachRemaining( r -> roles.add( (String) r.get( "role" ) ) );
            return roles;
        }
    }

    public static boolean userCanLogin( String username, String password, GraphDatabaseFacade db )
    {
        EnterpriseAuthManager authManager = authManager( db );
        AuthSubject subject = login( authManager, username, password );
        return AuthenticationResult.SUCCESS == subject.getAuthenticationResult();
    }

    private static AuthSubject login( EnterpriseAuthManager authManager, String username, String password )
    {
        EnterpriseLoginContext loginContext;
        try
        {
            loginContext = authManager.login( newBasicAuthToken( username, password ) );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new RuntimeException( e );
        }
        return loginContext.subject();
    }

    private static EnterpriseAuthManager authManager( GraphDatabaseFacade db )
    {
        return db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }
}
