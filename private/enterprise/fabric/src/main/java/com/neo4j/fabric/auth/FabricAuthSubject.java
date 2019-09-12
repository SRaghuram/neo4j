/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

public class FabricAuthSubject implements AuthSubject
{
    private final AuthSubject wrappedAuthSubject;
    private final Credentials credentials;

    public FabricAuthSubject( AuthSubject wrappedAuthSubject, Credentials credentials )
    {
        this.wrappedAuthSubject = wrappedAuthSubject;
        this.credentials = credentials;
    }

    @Override
    public void logout()
    {
        wrappedAuthSubject.logout();
    }

    @Override
    public AuthenticationResult getAuthenticationResult()
    {
        return wrappedAuthSubject.getAuthenticationResult();
    }

    @Override
    public void setPasswordChangeNoLongerRequired()
    {
        wrappedAuthSubject.setPasswordChangeNoLongerRequired();
    }

    @Override
    public boolean hasUsername( String username )
    {
        return wrappedAuthSubject.hasUsername( username );
    }

    @Override
    public String username()
    {
        return wrappedAuthSubject.username();
    }

    public Credentials getCredentials()
    {
        return credentials;
    }
}
