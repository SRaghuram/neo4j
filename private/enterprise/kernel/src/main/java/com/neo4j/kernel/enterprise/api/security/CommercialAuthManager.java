/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security;

import java.util.Map;

import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

public interface CommercialAuthManager extends AuthManager
{
    void clearAuthCache();

    @Override
    CommercialLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException;

    /**
     * Implementation that does no authentication.
     */
    CommercialAuthManager NO_AUTH = new CommercialAuthManager()
    {
        @Override
        public CommercialLoginContext login( Map<String,Object> authToken )
        {
            AuthToken.clearCredentials( authToken );
            return CommercialLoginContext.AUTH_DISABLED;
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
    };
}
