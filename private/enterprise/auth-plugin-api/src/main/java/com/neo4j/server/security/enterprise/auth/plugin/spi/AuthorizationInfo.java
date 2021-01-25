/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.spi;

import java.io.Serializable;
import java.util.Collection;

/**
 * An object that can be returned as the result of authorization by an {@link AuthorizationPlugin}.
 *
 * @see AuthorizationPlugin#authorize(Collection)
 */
public interface AuthorizationInfo extends Serializable
{
    /**
     * Should return a collection of roles assigned to the principals recognized by an {@link AuthorizationPlugin}.
     *
     * @return the roles assigned to the principals recognized by an {@link AuthorizationPlugin}.
     */
    Collection<String> roles();

    static AuthorizationInfo of( Collection<String> roles )
    {
        return () -> roles;
    }
}
