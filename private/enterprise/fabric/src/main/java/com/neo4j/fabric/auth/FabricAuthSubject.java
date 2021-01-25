/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import java.util.Map;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

public class FabricAuthSubject implements AuthSubject
{
    private final AuthSubject wrappedAuthSubject;
    private final Map<String,Object> interceptedAuthToken;

    public FabricAuthSubject( AuthSubject wrappedAuthSubject, Map<String,Object> interceptedAuthToken )
    {
        this.wrappedAuthSubject = wrappedAuthSubject;
        this.interceptedAuthToken = interceptedAuthToken;
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

    public Map<String,Object> getInterceptedAuthToken()
    {
        return interceptedAuthToken;
    }
}
