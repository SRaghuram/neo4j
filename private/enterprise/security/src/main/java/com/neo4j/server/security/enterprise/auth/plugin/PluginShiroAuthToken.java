/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.apache.shiro.authc.AuthenticationToken;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * Version of ShiroAuthToken that returns credentials as a char array
 * so that it is compatible for credentials matching with the
 * cacheable auth info results returned by the plugin API
 */
public class PluginShiroAuthToken extends ShiroAuthToken
{
    private final char[] credentials;

    private PluginShiroAuthToken( Map<String,Object> authTokenMap )
    {
        super( authTokenMap );
        // Convert credentials UTF8 byte[] to char[] (this should not create any intermediate copies)
        byte[] credentialsBytes = (byte[]) super.getCredentials();
        credentials = credentialsBytes != null ? StandardCharsets.UTF_8.decode( ByteBuffer.wrap( credentialsBytes ) ).array() : null;
    }

    @Override
    public Object getCredentials()
    {
        return credentials;
    }

    void clearCredentials()
    {
        if ( credentials != null )
        {
            Arrays.fill( credentials, (char) 0 );
        }
    }

    public static PluginShiroAuthToken of( ShiroAuthToken shiroAuthToken )
    {
        return new PluginShiroAuthToken( shiroAuthToken.getAuthTokenMap() );
    }

    public static PluginShiroAuthToken of( AuthenticationToken authenticationToken )
    {
        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) authenticationToken;
        return PluginShiroAuthToken.of( shiroAuthToken );
    }
}
