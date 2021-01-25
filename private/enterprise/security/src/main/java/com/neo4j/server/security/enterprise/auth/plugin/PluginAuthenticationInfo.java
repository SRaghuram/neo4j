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
import org.apache.shiro.util.ByteSource;

import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

class PluginAuthenticationInfo extends ShiroAuthenticationInfo implements CustomCredentialsMatcherSupplier
{
    private CustomCacheableAuthenticationInfo.CredentialsMatcher credentialsMatcher;

    private PluginAuthenticationInfo( Object principal, String realmName,
            CustomCacheableAuthenticationInfo.CredentialsMatcher credentialsMatcher )
    {
        super( principal, realmName, AuthenticationResult.SUCCESS );
        this.credentialsMatcher = credentialsMatcher;
    }

    private PluginAuthenticationInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName, AuthenticationResult.SUCCESS );
    }

    @Override
    public CustomCacheableAuthenticationInfo.CredentialsMatcher getCredentialsMatcher()
    {
        return credentialsMatcher;
    }

    private static PluginAuthenticationInfo create(
            AuthenticationInfo authenticationInfo,
            String realmName )
    {
        return new PluginAuthenticationInfo( authenticationInfo.principal(), realmName, null );
    }

    private static PluginAuthenticationInfo create(
            AuthenticationInfo authenticationInfo,
            SimpleHash hashedCredentials,
            String realmName )
    {
        return new PluginAuthenticationInfo(
                            authenticationInfo.principal(),
                            hashedCredentials.getBytes(),
                            hashedCredentials.getSalt(),
                            realmName );
    }

    public static PluginAuthenticationInfo createCacheable(
            AuthenticationInfo authenticationInfo,
            String realmName,
            SecureHasher secureHasher )
    {
        if ( authenticationInfo instanceof CustomCacheableAuthenticationInfo )
        {
            CustomCacheableAuthenticationInfo info = (CustomCacheableAuthenticationInfo) authenticationInfo;
            return new PluginAuthenticationInfo( authenticationInfo.principal(), realmName, info.credentialsMatcher() );
        }
        else if ( authenticationInfo instanceof CacheableAuthenticationInfo )
        {
            byte[] credentials = ((CacheableAuthenticationInfo) authenticationInfo).credentials();
            SimpleHash hashedCredentials = secureHasher.hash( credentials );
            return PluginAuthenticationInfo.create( authenticationInfo, hashedCredentials, realmName );
        }
        else
        {
            return PluginAuthenticationInfo.create( authenticationInfo, realmName );
        }
    }
}
