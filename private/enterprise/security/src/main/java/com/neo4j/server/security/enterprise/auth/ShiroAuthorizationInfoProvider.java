/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.subject.PrincipalCollection;

public interface ShiroAuthorizationInfoProvider
{
    AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principal );
}
