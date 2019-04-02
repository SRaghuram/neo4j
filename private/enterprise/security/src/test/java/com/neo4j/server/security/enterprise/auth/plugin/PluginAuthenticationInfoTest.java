/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import org.neo4j.server.security.auth.SecureHasher;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.junit.Test;

import java.util.List;

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PluginAuthenticationInfoTest
{
    @Test
    public void shouldCreateCorrectAuthenticationInfo()
    {
        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable( AuthenticationInfo.of( "thePrincipal" ), "theRealm", null );

        assertThat( (List<String>)internalAuthInfo.getPrincipals().asList(), containsInAnyOrder( "thePrincipal" ) );
    }

    @Test
    public void shouldCreateCorrectAuthenticationInfoFromCacheable()
    {
        SecureHasher hasher = mock( SecureHasher.class );
        when( hasher.hash( any() ) ).thenReturn( new SimpleHash( "some-hash" ) );

        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable(
                        CacheableAuthenticationInfo.of( "thePrincipal", new byte[]{1} ),
                        "theRealm",
                        hasher
                );

        assertThat( (List<String>)internalAuthInfo.getPrincipals().asList(), containsInAnyOrder( "thePrincipal" ) );
    }

    @Test
    public void shouldCreateCorrectAuthenticationInfoFromCustomCacheable()
    {
        SecureHasher hasher = mock( SecureHasher.class );
        when( hasher.hash( any() ) ).thenReturn( new SimpleHash( "some-hash" ) );

        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable(
                        CustomCacheableAuthenticationInfo.of( "thePrincipal", ignoredAuthToken -> true ),
                        "theRealm",
                        hasher
                );

        assertThat( (List<String>)internalAuthInfo.getPrincipals().asList(), containsInAnyOrder( "thePrincipal" ) );
    }
}
