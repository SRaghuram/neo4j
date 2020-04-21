/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

import java.util.Arrays;
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
        boolean authProvided = !authToken.get( AuthToken.SCHEME_KEY ).equals( "none" );
        String username = null;
        byte[] password = null;

        if ( authToken.containsKey( AuthToken.PRINCIPAL ) && authToken.containsKey( AuthToken.CREDENTIALS ) )
        {
            username = AuthToken.safeCast( AuthToken.PRINCIPAL, authToken );
            byte[] originalPassword = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, authToken );
            // the original password is erased after the authentication
            password = Arrays.copyOf( originalPassword, originalPassword.length );
        }

        EnterpriseLoginContext wrappedLoginContext = wrappedAuthManager.login( authToken );

        Credentials credentials = new Credentials( username, password, authProvided );
        FabricAuthSubject fabricAuthSubject = new FabricAuthSubject( wrappedLoginContext.subject(), credentials );
        return new FabricLoginContext( wrappedLoginContext, fabricAuthSubject );
    }

    public static Credentials getCredentials( AuthSubject authSubject )
    {
        if ( !(authSubject instanceof FabricAuthSubject) )
        {
            throw new IllegalArgumentException( "The submitted subject was not created by Fabric Authentication manager: " + authSubject );
        }

        return ((FabricAuthSubject) authSubject).getCredentials();
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

}
