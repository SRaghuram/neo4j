/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import com.neo4j.server.security.enterprise.auth.plugin.api.AuthorizationExpiredException;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class TestCombinedAuthPlugin extends AuthenticationPlugin.Adapter implements AuthorizationPlugin
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return AuthenticationInfo.of( "neo4j" );
        }
        else if ( principal.equals( "authorization_expired_user" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return (AuthenticationInfo) () -> "authorization_expired_user";
        }
        return null;
    }

    @Override
    public AuthorizationInfo authorize( Collection<PrincipalAndProvider> principals )
    {
        if ( principals.stream().anyMatch( p -> "neo4j".equals( p.principal() ) ) )
        {
            return (AuthorizationInfo) () -> Collections.singleton( PredefinedRoles.READER );
        }
        else if ( principals.stream().anyMatch( p -> "authorization_expired_user".equals( p.principal() ) ) )
        {
            throw new AuthorizationExpiredException( "authorization_expired_user needs to re-authenticate." );
        }
        return null;
    }
}
