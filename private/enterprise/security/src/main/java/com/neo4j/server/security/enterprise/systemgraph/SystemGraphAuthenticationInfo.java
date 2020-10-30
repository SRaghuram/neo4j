/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.impl.security.User;

/**
 * This is used by SystemGraphRealm to cache a user record in the authentication caches
 * and update the authentication result based on the outcome of its CredentialsMatcher
 */
class SystemGraphAuthenticationInfo extends ShiroAuthenticationInfo
{
    private final User userRecord;

    SystemGraphAuthenticationInfo( User userRecord, String realmName )
    {
        super( userRecord.name(), realmName, AuthenticationResult.FAILURE );
        this.userRecord = userRecord;
    }

    User getUserRecord()
    {
        return userRecord;
    }

    void setAuthenticationResult( AuthenticationResult authenticationResult )
    {
        this.authenticationResult = authenticationResult;
    }
}
