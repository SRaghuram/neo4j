/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;

public class TestCacheableAuthenticationPlugin extends AuthenticationPlugin.CachingEnabledAdapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        getAuthenticationInfoCallCount.incrementAndGet();

        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return CacheableAuthenticationInfo.of( "neo4j", "neo4j".getBytes() );
        }
        return null;
    }

    public static AtomicInteger getAuthenticationInfoCallCount = new AtomicInteger( 0 );
}
