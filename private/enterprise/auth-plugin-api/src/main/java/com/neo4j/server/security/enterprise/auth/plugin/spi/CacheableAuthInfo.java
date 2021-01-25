/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.spi;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

import java.util.Collection;

/**
 * A cacheable object that can be returned as the result of successful authentication by an
 * {@link AuthPlugin}.
 *
 * <p>This object can be cached by the Neo4j authentication cache.
 *
 * <p>This result type is used if you want Neo4j to manage secure hashing and matching of cached credentials.
 * If you instead want to manage this yourself you need to use the separate interfaces
 * {@link AuthenticationPlugin} and {@link AuthorizationPlugin} together with
 * a {@link CustomCacheableAuthenticationInfo} result.
 *
 * <p>NOTE: Caching of authentication info only occurs if it is explicitly enabled by the plugin, whereas
 * caching of authorization info (assigned roles) is enabled by default.
 *
 * <p>NOTE: Caching of the authorization info (assigned roles) does not require the use of a {@link CacheableAuthInfo}
 * but will work fine with a regular {@link AuthInfo}.
 *
 * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
 * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
 * @see AuthInfo
 * @see AuthenticationPlugin
 * @see AuthorizationPlugin
 * @see CustomCacheableAuthenticationInfo
 */
public interface CacheableAuthInfo extends AuthInfo
{
    /**
     * Should return a principal that uniquely identifies the authenticated subject within this auth provider.
     * This will be used as the cache key, and needs to be matcheable against a principal within the auth token map.
     *
     * <p>Typically this is the same as the principal within the auth token map.
     *
     * @return a principal that uniquely identifies the authenticated subject within this auth provider
     *
     * @see AuthToken#principal()
     */
    @Override
    Object principal();

    /**
     * Should return credentials that can be cached, so that successive authentication attempts could be performed
     * against the cached authentication info from a previous successful authentication attempt.
     *
     * <p>NOTE: The returned credentials will be hashed using a cryptographic hash function together
     * with a random salt (generated with a secure random number generator) before being stored.
     *
     * @return credentials that can be cached
     *
     * @see AuthToken#credentials()
     * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
     */
    byte[] credentials();

    static CacheableAuthInfo of( Object principal, byte[] credentials, Collection<String> roles )
    {
        return new CacheableAuthInfo()
        {
            @Override
            public Object principal()
            {
                return principal;
            }

            @Override
            public byte[] credentials()
            {
                return credentials;
            }

            @Override
            public Collection<String> roles()
            {
                return roles;
            }
        };
    }
}
