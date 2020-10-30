/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.support.DelegatingSubject;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

public class ShiroSubject extends DelegatingSubject
{
    private AuthenticationResult authenticationResult;
    private ShiroAuthenticationInfo authenticationInfo;

    public ShiroSubject( SecurityManager securityManager, AuthenticationResult authenticationResult )
    {
        super( securityManager );
        this.authenticationResult = authenticationResult;
    }

    public ShiroSubject( PrincipalCollection principals, boolean authenticated, String host, Session session,
            boolean sessionCreationEnabled, SecurityManager securityManager, AuthenticationResult authenticationResult,
            ShiroAuthenticationInfo authenticationInfo )
    {
        super( principals, authenticated, host, session, sessionCreationEnabled, securityManager );
        this.authenticationResult = authenticationResult;
        this.authenticationInfo = authenticationInfo;
    }

    public AuthenticationResult getAuthenticationResult()
    {
        return authenticationResult;
    }

    void setAuthenticationResult( AuthenticationResult authenticationResult )
    {
        this.authenticationResult = authenticationResult;
    }

    public ShiroAuthenticationInfo getAuthenticationInfo()
    {
        return authenticationInfo;
    }

    public void clearAuthenticationInfo()
    {
        this.authenticationInfo = null;
    }
}
