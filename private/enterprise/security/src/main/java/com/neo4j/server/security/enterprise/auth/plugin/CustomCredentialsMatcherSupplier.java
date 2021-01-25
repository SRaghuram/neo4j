/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

public interface CustomCredentialsMatcherSupplier
{
    CustomCacheableAuthenticationInfo.CredentialsMatcher getCredentialsMatcher();
}
