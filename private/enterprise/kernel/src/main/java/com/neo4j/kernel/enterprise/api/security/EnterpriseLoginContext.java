/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;
import java.util.function.ToIntFunction;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;

public interface EnterpriseLoginContext extends LoginContext
{
    Set<String> roles();

    @Override
    EnterpriseSecurityContext authorize( ToIntFunction<String> propertyIdLookup, String dbName );

    EnterpriseLoginContext AUTH_DISABLED = new EnterpriseLoginContext()
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
        public EnterpriseSecurityContext authorize( ToIntFunction<String> propertyIdLookup, String dbName )
        {
            return EnterpriseSecurityContext.AUTH_DISABLED;
        }
    };
}
