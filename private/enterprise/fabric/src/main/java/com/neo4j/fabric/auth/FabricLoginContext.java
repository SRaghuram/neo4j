/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;

import java.util.Set;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.AuthSubject;

public class FabricLoginContext implements CommercialLoginContext
{

    private final CommercialLoginContext wrappedLoginContext;
    private final FabricAuthSubject authSubject;

    public FabricLoginContext( CommercialLoginContext wrappedLoginContext, FabricAuthSubject authSubject )
    {
        this.wrappedLoginContext = wrappedLoginContext;
        this.authSubject = authSubject;
    }

    @Override
    public Set<String> roles()
    {
        return wrappedLoginContext.roles();
    }

    @Override
    public AuthSubject subject()
    {
        return authSubject;
    }

    @Override
    public CommercialSecurityContext authorize( IdLookup idLookup, String dbName ) throws KernelException
    {
        return wrappedLoginContext.authorize( idLookup, dbName );
    }
}
