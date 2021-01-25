/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import com.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

public class PluginAuthInfo extends ShiroAuthenticationInfo implements AuthorizationInfo
{
    Set<String> roles;

    private PluginAuthInfo( Object principal, String realmName, Set<String> roles )
    {
        super( principal, realmName, AuthenticationResult.SUCCESS );
        this.roles = roles;
    }

    private PluginAuthInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName, Set<String> roles )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName, AuthenticationResult.SUCCESS );
        this.roles = roles;
    }

    private PluginAuthInfo( AuthInfo authInfo, SimpleHash hashedCredentials, String realmName )
    {
        this( authInfo.principal(), hashedCredentials.getBytes(), hashedCredentials.getSalt(), realmName,
                new HashSet<>( authInfo.roles() ) );
    }

    public static PluginAuthInfo create( AuthInfo authInfo, String realmName )
    {
        return new PluginAuthInfo( authInfo.principal(), realmName, new HashSet<>( authInfo.roles() ) );
    }

    public static PluginAuthInfo createCacheable( AuthInfo authInfo, String realmName, SecureHasher secureHasher )
    {
        if ( authInfo instanceof CacheableAuthInfo )
        {
            byte[] credentials = ((CacheableAuthInfo) authInfo).credentials();
            SimpleHash hashedCredentials = secureHasher.hash( credentials );
            return new PluginAuthInfo( authInfo, hashedCredentials, realmName );
        }
        else
        {
            return new PluginAuthInfo( authInfo.principal(), realmName, new HashSet<>( authInfo.roles() ) );
        }
    }

    @Override
    public Collection<String> getRoles()
    {
        return roles;
    }

    @Override
    public Collection<String> getStringPermissions()
    {
        return Collections.emptyList();
    }

    @Override
    public Collection<Permission> getObjectPermissions()
    {
        return Collections.emptyList();
    }
}
