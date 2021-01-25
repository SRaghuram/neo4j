/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCacheableAdminAuthPlugin extends AuthPlugin.CachingEnabledAdapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthInfo authenticateAndAuthorize( AuthToken authToken )
    {
        GET_AUTH_INFO_CALL_COUNT.incrementAndGet();

        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return CacheableAuthInfo.of( "neo4j", "neo4j".getBytes(),
                    Collections.singleton( PredefinedRoles.ADMIN ) );
        }
        return null;
    }

    // For testing purposes
    private static final AtomicInteger GET_AUTH_INFO_CALL_COUNT = new AtomicInteger( 0 );
}
