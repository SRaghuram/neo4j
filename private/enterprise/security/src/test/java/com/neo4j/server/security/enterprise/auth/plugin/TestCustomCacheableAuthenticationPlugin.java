/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCustomCacheableAuthenticationPlugin extends AuthenticationPlugin.CachingEnabledAdapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        GET_AUTHENTICATION_INFO_CALL_COUNT.incrementAndGet();

        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return CustomCacheableAuthenticationInfo.of( "neo4j",
                    token ->
                    {
                        char[] tokenCredentials = token.credentials();
                        return Arrays.equals( tokenCredentials, "neo4j".toCharArray() );
                    } );
        }
        return null;
    }

    // For testing purposes
    public static final AtomicInteger GET_AUTHENTICATION_INFO_CALL_COUNT = new AtomicInteger( 0 );
}
