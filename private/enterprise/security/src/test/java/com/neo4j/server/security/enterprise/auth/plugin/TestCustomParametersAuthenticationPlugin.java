/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;

public class TestCustomParametersAuthenticationPlugin extends AuthenticationPlugin.Adapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        Map<String,Object> parameters = authToken.parameters();

        List<Long> myCredentials = (List<Long>) parameters.get( "my_credentials" );

        if ( myCredentials.containsAll( Arrays.asList( 1L, 2L, 3L, 4L ) ) )
        {
            return (AuthenticationInfo) () -> "neo4j";
        }
        return null;
    }
}
