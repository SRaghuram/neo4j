/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import java.util.Collection;
import java.util.Collections;

import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

public class TestAuthorizationPlugin extends AuthorizationPlugin.Adapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthorizationInfo authorize( Collection<PrincipalAndProvider> principals )
    {
        if ( principals.stream().anyMatch( p -> "neo4j".equals( p.principal() ) ) )
        {
            return (AuthorizationInfo) () -> Collections.singleton( PredefinedRoles.READER );
        }
        return null;
    }
}
