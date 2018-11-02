/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin.spi;

import java.io.Serializable;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

/**
 * An object that can be returned as the result of successful authentication by an {@link AuthenticationPlugin}.
 *
 * @see AuthenticationPlugin#authenticate(AuthToken)
 */
public interface AuthenticationInfo extends Serializable
{
    /**
     * Should return a principal that uniquely identifies the authenticated subject within this authentication
     * provider.
     *
     * <p>Typically this is the same as the principal within the auth token map.
     *
     * @return a principal that uniquely identifies the authenticated subject within this authentication provider.
     */
    Object principal();

    static AuthenticationInfo of( Object principal )
    {
        return (AuthenticationInfo) () -> principal;
    }
}
