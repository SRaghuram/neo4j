/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.InClusterAuthManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

public class ClusterCredentialsProvider implements CredentialsProvider
{
    @Override
    public AuthToken credentialsFor( EnterpriseLoginContext loginContext )
    {
        String username = loginContext.subject().username();
        List<String> roles = new ArrayList<>( loginContext.roles() );

        return AuthTokens.custom( username, "", null, InClusterAuthManager.SCHEME, Map.of( InClusterAuthManager.ROLES_KEY, roles ) );
    }
}
