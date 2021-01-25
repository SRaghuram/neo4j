/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.cypher.internal.security.SecureHasher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginAuthenticationInfoTest
{
    @Test
    void shouldCreateCorrectAuthenticationInfo()
    {
        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable( AuthenticationInfo.of( "thePrincipal" ), "theRealm", null );

        assertThat( (List<String>) internalAuthInfo.getPrincipals().asList() ).contains( "thePrincipal" );
    }

    @Test
    void shouldCreateCorrectAuthenticationInfoFromCacheable()
    {
        SecureHasher hasher = mock( SecureHasher.class );
        when( hasher.hash( any() ) ).thenReturn( new SimpleHash( "some-hash" ) );

        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable(
                        CacheableAuthenticationInfo.of( "thePrincipal", new byte[]{1} ),
                        "theRealm",
                        hasher
                );

        assertThat( (List<String>) internalAuthInfo.getPrincipals().asList() ).contains( "thePrincipal" );
    }

    @Test
    void shouldCreateCorrectAuthenticationInfoFromCustomCacheable()
    {
        SecureHasher hasher = mock( SecureHasher.class );
        when( hasher.hash( any() ) ).thenReturn( new SimpleHash( "some-hash" ) );

        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable(
                        CustomCacheableAuthenticationInfo.of( "thePrincipal", ignoredAuthToken -> true ),
                        "theRealm",
                        hasher
                );

        assertThat( (List<String>) internalAuthInfo.getPrincipals().asList() ).contains( "thePrincipal" );
    }
}
