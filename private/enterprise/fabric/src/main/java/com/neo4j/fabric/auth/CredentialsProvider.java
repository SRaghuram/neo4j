/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.internal.kernel.api.security.AuthSubject;

public class CredentialsProvider
{

    public AuthToken credentialsFor( AuthSubject subject )
    {
        Credentials credentials = FabricAuthManagerWrapper.getCredentials( subject );
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
