/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

import org.neo4j.driver.AuthToken;

public interface CredentialsProvider
{

    AuthToken credentialsFor( EnterpriseLoginContext loginContext );
}
