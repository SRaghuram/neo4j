/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import java.util.Arrays;
import java.util.Collections;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;

public class TestAuthPlugin extends AuthPlugin.Adapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthInfo authenticateAndAuthorize( AuthToken authToken )
    {
        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return AuthInfo.of( "neo4j", Collections.singleton( PredefinedRoles.READER ) );
        }
        return null;
    }
}
