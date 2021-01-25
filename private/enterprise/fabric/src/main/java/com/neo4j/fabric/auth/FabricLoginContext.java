/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.Set;

import org.neo4j.internal.kernel.api.security.AuthSubject;

public class FabricLoginContext implements EnterpriseLoginContext
{

    private final EnterpriseLoginContext wrappedLoginContext;
    private final FabricAuthSubject authSubject;

    public FabricLoginContext( EnterpriseLoginContext wrappedLoginContext, FabricAuthSubject authSubject )
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
    public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
    {
        return wrappedLoginContext.authorize( idLookup, dbName );
    }
}
