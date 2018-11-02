/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import java.util.Arrays;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

public class TestCredentialsOnlyPlugin extends AuthenticationPlugin.Adapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        String username = validateCredentials( authToken.credentials() );
        return new AuthenticationInfo( username, authToken.credentials() );
    }

    /**
     * Performs decryptions of the credentials and returns the decrypted username if successful
     */
    private String validateCredentials( char[] credentials )
    {
        return "trinity@MATRIX.NET";
    }

    class AuthenticationInfo implements CustomCacheableAuthenticationInfo, CustomCacheableAuthenticationInfo.CredentialsMatcher
    {
        private final String username;
        private final char[] credentials;

        AuthenticationInfo( String username, char[] credentials )
        {
            this.username = username;
            // Since the credentials array will be cleared we make need to make a copy here
            // (in a real world scenario you would probably not store this copy in clear text)
            this.credentials = credentials.clone();
        }

        @Override
        public Object principal()
        {
            return username;
        }

        @Override
        public CredentialsMatcher credentialsMatcher()
        {
            return this;
        }

        @Override
        public boolean doCredentialsMatch( AuthToken authToken )
        {
            return Arrays.equals( authToken.credentials(), credentials );
        }
    }
}
