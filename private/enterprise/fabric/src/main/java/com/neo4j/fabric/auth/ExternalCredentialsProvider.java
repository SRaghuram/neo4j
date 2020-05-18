/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

public class ExternalCredentialsProvider implements CredentialsProvider
{
    @Override
    public AuthToken credentialsFor( EnterpriseLoginContext loginContext )
    {
        Credentials credentials = FabricAuthManagerWrapper.getCredentials( loginContext.subject() );
        if ( credentials.getProvided() )
        {
            return AuthTokens.basic( credentials.getUsername(), new String( credentials.getPassword() ) );
        }
        else
        {
            return AuthTokens.none();
        }
    }
}
