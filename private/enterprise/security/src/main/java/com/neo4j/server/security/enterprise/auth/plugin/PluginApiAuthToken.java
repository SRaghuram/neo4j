/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.neo4j.kernel.api.security.AuthToken.CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.PARAMETERS;
import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;

public class PluginApiAuthToken implements AuthToken
{
    private final String principal;
    private final char[] credentials;
    private final Map<String,Object> parameters;

    private PluginApiAuthToken( String principal, char[] credentials, Map<String,Object> parameters )
    {
        this.principal = principal;
        this.credentials = credentials;
        this.parameters = parameters;
    }

    @Override
    public String principal()
    {
        return principal;
    }

    @Override
    public char[] credentials()
    {
        return credentials;
    }

    @Override
    public Map<String,Object> parameters()
    {
        return parameters;
    }

    void clearCredentials()
    {
        if ( credentials != null )
        {
            Arrays.fill( credentials, (char) 0 );
        }
    }

    public static PluginApiAuthToken of( String principal, char[] credentials, Map<String,Object> parameters )
    {
        return new PluginApiAuthToken( principal, credentials, parameters );
    }

    public static PluginApiAuthToken createFromMap( Map<String,Object> authTokenMap ) throws InvalidAuthTokenException
    {
        String scheme = org.neo4j.kernel.api.security.AuthToken
                .safeCast( org.neo4j.kernel.api.security.AuthToken.SCHEME_KEY, authTokenMap );

        // Always require principal
        String principal = org.neo4j.kernel.api.security.AuthToken.safeCast( PRINCIPAL, authTokenMap );

        byte[] credentials = null;
        if ( scheme.equals( org.neo4j.kernel.api.security.AuthToken.BASIC_SCHEME ) )
        {
            // Basic scheme requires credentials
            credentials = org.neo4j.kernel.api.security.AuthToken.safeCastCredentials( CREDENTIALS, authTokenMap );
        }
        else
        {
            // Otherwise credentials are optional
            Object credentialsObject = authTokenMap.get( CREDENTIALS );
            if ( credentialsObject instanceof byte[] )
            {
                credentials = (byte[]) credentialsObject;
            }
        }
        Map<String,Object> parameters = org.neo4j.kernel.api.security.AuthToken.safeCastMap( PARAMETERS, authTokenMap );

        return PluginApiAuthToken.of(
                principal,
                // Convert UTF8 byte[] to char[] (this should not create any intermediate copies)
                credentials != null ? StandardCharsets.UTF_8.decode( ByteBuffer.wrap( credentials ) ).array() : null,
                parameters );
    }
}
