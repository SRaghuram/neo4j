/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Map;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.Credential;

public interface EnterpriseAuthManager extends AuthManager
{
    void clearAuthCache();

    @Override
    EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException;

    /**
     * Implementation that does no authentication.
     */
    EnterpriseAuthManager NO_AUTH = new EnterpriseAuthManager()
    {
        @Override
        public EnterpriseLoginContext login( Map<String,Object> authToken )
        {
            AuthToken.clearCredentials( authToken );
            return EnterpriseLoginContext.AUTH_DISABLED;
        }

        @Override
        public void init()
        {
        }

        @Override
        public void start()
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void shutdown()
        {
        }

        @Override
        public void clearAuthCache()
        {
        }

        @Override
        public Credential createCredentialForPassword( byte[] password )
        {
            return Credential.INACCESSIBLE;
        }

        @Override
        public Credential deserialize( String part )
        {
            return Credential.INACCESSIBLE;
        }

        @Override
        public void log( String message, SecurityContext securityContext )
        {
        }
    };
}
