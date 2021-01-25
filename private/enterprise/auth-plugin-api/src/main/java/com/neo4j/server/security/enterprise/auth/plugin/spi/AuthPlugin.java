/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.spi;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import com.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import com.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;

/**
 * A simplified combined authentication and authorization provider plugin for the Neo4j enterprise security module.
 *
 * <p>If either the configuration setting {@code dbms.security.plugin.authentication_enabled} or
 * {@code dbms.security.plugin.authorization_enabled} is set to {@code true},
 * all objects that implements this interface that exists in the class path at Neo4j startup, will be
 * loaded as services.
 *
 * @see AuthenticationPlugin
 * @see AuthorizationPlugin
 */
public interface AuthPlugin extends AuthProviderLifecycle
{
    /**
     * The name of this auth provider.
     *
     * <p>This name, prepended with the prefix "plugin-", can be used by a client to direct an auth token directly
     * to this auth provider.
     *
     * @return the name of this auth provider
     */
    String name();

    /**
     * Should perform both authentication and authorization of the identity in the given auth token and return an
     * {@link AuthInfo} result if successful. The {@link AuthInfo} result can also contain a collection of roles
     * that are assigned to the given identity, which constitutes the authorization part.
     *
     * If authentication failed, either {@code null} should be returned,
     * or an {@link AuthenticationException} should be thrown.
     *
     * <p>If authentication caching is enabled, then a {@link CacheableAuthInfo} should be returned.
     *
     * @return an {@link AuthInfo} object if authentication was successful, otherwise {@code null}
     *
     * @see AuthToken
     * @see AuthenticationInfo
     * @see CacheableAuthenticationInfo
     * @see CustomCacheableAuthenticationInfo
     * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
     */
    AuthInfo authenticateAndAuthorize( AuthToken authToken ) throws AuthenticationException;

    abstract class Adapter extends AuthProviderLifecycle.Adapter implements AuthPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }
    }

    abstract class CachingEnabledAdapter extends AuthProviderLifecycle.Adapter implements AuthPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }

        @Override
        public void initialize( AuthProviderOperations authProviderOperations )
        {
            authProviderOperations.setAuthenticationCachingEnabled( true );
        }
    }
}
