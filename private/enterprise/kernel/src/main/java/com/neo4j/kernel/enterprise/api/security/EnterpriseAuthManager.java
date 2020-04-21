/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Map;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

public abstract class EnterpriseAuthManager extends AuthManager
{
    public abstract void clearAuthCache();

    @Override
    public abstract EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException;

    /**
     * Implementation that does no authentication.
     */
    public static final EnterpriseAuthManager NO_AUTH = new EnterpriseAuthManager()
    {
        @Override
        public EnterpriseLoginContext login( Map<String,Object> authToken )
        {
            AuthToken.clearCredentials( authToken );
            return EnterpriseLoginContext.AUTH_DISABLED;
        }

        @Override
        public void clearAuthCache()
        {
        }

        @Override
        public void log( String message, SecurityContext securityContext )
        {
        }
    };
}
