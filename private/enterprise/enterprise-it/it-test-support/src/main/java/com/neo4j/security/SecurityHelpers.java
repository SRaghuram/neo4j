/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;

import java.util.Set;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

public class SecurityHelpers
{
    public static void newUser( GraphDatabaseFacade db, String username, String password )
    {
        try
        {
            userManager( db ).newUser( username, UTF8.encode( password ), false );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public static Set<String> getAllRoleNames( GraphDatabaseFacade db )
    {
        try
        {
            return userManager( db ).getAllRoleNames();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
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

    private static EnterpriseUserManager userManager( GraphDatabaseFacade db )
    {
        EnterpriseAuthManager commercialAuthManager = authManager( db );
        if ( commercialAuthManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) commercialAuthManager).getUserManager();
        }
        throw new RuntimeException( "The configuration used does not have a user manager" );
    }

    private static EnterpriseAuthManager authManager( GraphDatabaseFacade db )
    {
        return db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }
}
