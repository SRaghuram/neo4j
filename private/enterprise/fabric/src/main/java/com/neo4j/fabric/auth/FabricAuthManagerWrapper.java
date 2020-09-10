/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

public class FabricAuthManagerWrapper extends EnterpriseAuthManager
{
    private final EnterpriseAuthManager wrappedAuthManager;

    public FabricAuthManagerWrapper( EnterpriseAuthManager wrappedAuthManager )
    {
        this.wrappedAuthManager = wrappedAuthManager;
    }

    @Override
    public EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        var copiedAuthToken = copyAuthToken( authToken );

        EnterpriseLoginContext wrappedLoginContext = wrappedAuthManager.login( authToken );

        FabricAuthSubject fabricAuthSubject = new FabricAuthSubject( wrappedLoginContext.subject(), copiedAuthToken );
        return new FabricLoginContext( wrappedLoginContext, fabricAuthSubject );
    }

    private Map<String,Object> copyAuthToken( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        Map<String,Object> copiedToken = new HashMap<>();

        for ( var entry : authToken.entrySet() )
        {
            if ( AuthToken.CREDENTIALS.equals( entry.getKey() ) )
            {
                byte[] originalCredentials = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, authToken );
                // the original credentials are erased after the authentication
                byte[] copiedCredentials = Arrays.copyOf( originalCredentials, originalCredentials.length );
                copiedToken.put( entry.getKey(), copiedCredentials );
            }
            else
            {
                copiedToken.put( entry.getKey(), entry.getValue() );
            }
        }

        return copiedToken;
    }

    public static Map<String,Object> getInterceptedAuthToken( AuthSubject authSubject )
    {
        if ( !(authSubject instanceof FabricAuthSubject) )
        {
            throw new IllegalArgumentException( "The submitted subject was not created by Fabric Authentication manager: " + authSubject );
        }

        return ((FabricAuthSubject) authSubject).getInterceptedAuthToken();
    }

    @Override
    public void clearAuthCache()
    {
        wrappedAuthManager.clearAuthCache();
    }

    @Override
    public void log( String message, SecurityContext securityContext )
    {
        wrappedAuthManager.log( message, securityContext );
    }

    @Override
    public List<Map<String,String>> getPrivilegesGrantedThroughConfig()
    {
        return wrappedAuthManager.getPrivilegesGrantedThroughConfig();
    }
}
