/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.api.security.provider;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;

import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public final class EnterpriseNoAuthSecurityProvider extends LifecycleAdapter implements SecurityProvider
{
    public static final EnterpriseNoAuthSecurityProvider INSTANCE = new EnterpriseNoAuthSecurityProvider();

    private EnterpriseNoAuthSecurityProvider()
    {
    }

    @Override
    public AuthManager authManager()
    {
        return EnterpriseAuthManager.NO_AUTH;
    }

    @Override
    public AuthManager inClusterAuthManager()
    {
        return EnterpriseAuthManager.NO_AUTH;
    }
}
