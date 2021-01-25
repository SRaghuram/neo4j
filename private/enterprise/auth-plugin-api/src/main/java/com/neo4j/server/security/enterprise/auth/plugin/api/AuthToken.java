/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.api;

import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;

import java.util.Map;

/**
 * The authentication token provided by the client, which is used to authenticate the subject's identity.
 *
 * <p>A common scenario is to have principal be a username and credentials be a password.
 *
 * @see AuthenticationPlugin#authenticate(AuthToken)
 * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
 */
public interface AuthToken
{
    /**
     * Returns the identity to authenticate.
     *
     * <p>Most commonly this is a username.
     *
     * @return the identity to authenticate.
     */
    String principal();

    /**
     * Returns the credentials that verifies the identity.
     *
     * <p>Most commonly this is a password.
     *
     * <p>The reason this is a character array and not a {@link String}, is so that sensitive information
     * can be cleared from memory after usage without having to wait for the garbage collector to act.
     *
     * @return the credentials that verifies the identity.
     */
    char[] credentials();

    /**
     * Returns an optional custom parameter map if provided by the client.
     *
     * <p>This can be used as a vehicle to send arbitrary auth data from a client application
     * to a server-side auth plugin. Neo4j will act as a pure transport and will not touch the contents of this map.
     *
     * @return a custom parameter map (or an empty map if no parameters where provided by the client)
     */
    Map<String,Object> parameters();
}
