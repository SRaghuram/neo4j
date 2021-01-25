/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.api;

import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

/**
 * An exception that can be thrown if authorization has expired and the user needs to re-authenticate
 * in order to renew authorization.
 * Throwing this exception will cause the server to disconnect the client.
 *
 * <p>This is typically used from the {@link AuthorizationPlugin#authorize}
 * method of a combined authentication and authorization plugin (that implements the two separate interfaces
 * {@link AuthenticationPlugin} and {@link AuthorizationPlugin}), that manages its own caching of auth info.
 *
 * @see AuthorizationPlugin
 */
public class AuthorizationExpiredException extends RuntimeException
{
    public AuthorizationExpiredException( String message )
    {
        super( message );
    }
}
