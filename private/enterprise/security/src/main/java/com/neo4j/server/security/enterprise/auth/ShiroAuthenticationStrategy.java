/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.pam.AbstractAuthenticationStrategy;
import org.apache.shiro.realm.Realm;

import java.util.Collection;

import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

public class ShiroAuthenticationStrategy extends AbstractAuthenticationStrategy
{
    @Override
    public AuthenticationInfo beforeAllAttempts( Collection<? extends Realm> realms, AuthenticationToken token )
            throws AuthenticationException
    {
        return new ShiroAuthenticationInfo();
    }

    @Override
    public AuthenticationInfo afterAttempt( Realm realm, AuthenticationToken token, AuthenticationInfo singleRealmInfo,
            AuthenticationInfo aggregateInfo, Throwable t ) throws AuthenticationException
    {
        AuthenticationInfo info = super.afterAttempt( realm, token, singleRealmInfo, aggregateInfo, t );
        if ( t != null && info instanceof ShiroAuthenticationInfo )
        {
            // Save the throwable so we can use it for correct log messages later
            ((ShiroAuthenticationInfo) info).addThrowable( t );
        }
        return info;
    }
}
