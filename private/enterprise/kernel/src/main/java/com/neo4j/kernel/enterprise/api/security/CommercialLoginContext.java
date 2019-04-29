/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;

public interface CommercialLoginContext extends LoginContext
{
    Set<String> roles();

    @Override
    CommercialSecurityContext authorize( IdLookup idLookup, String dbName ) throws KernelException;

    CommercialLoginContext AUTH_DISABLED = new CommercialLoginContext()
    {
        @Override
        public AuthSubject subject()
        {
            return AuthSubject.AUTH_DISABLED;
        }

        @Override
        public Set<String> roles()
        {
            return Collections.emptySet();
        }

        @Override
        public CommercialSecurityContext authorize( IdLookup idLookup, String dbName )
        {
            return CommercialSecurityContext.AUTH_DISABLED;
        }
    };
}
