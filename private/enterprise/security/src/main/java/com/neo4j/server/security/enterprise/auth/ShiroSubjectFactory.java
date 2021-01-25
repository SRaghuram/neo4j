/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.mgt.SubjectFactory;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;

import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

public class ShiroSubjectFactory implements SubjectFactory
{
    @Override
    public Subject createSubject( SubjectContext context )
    {
        SecurityManager securityManager = context.resolveSecurityManager();
        Session session = context.resolveSession();
        boolean sessionCreationEnabled = context.isSessionCreationEnabled();
        PrincipalCollection principals = context.resolvePrincipals();
        boolean authenticated = context.resolveAuthenticated();
        String host = context.resolveHost();
        ShiroAuthenticationInfo authcInfo = (ShiroAuthenticationInfo) context.getAuthenticationInfo();

        return new ShiroSubject( principals, authenticated, host, session, sessionCreationEnabled, securityManager,
                authcInfo.getAuthenticationResult(), authcInfo );
    }
}
