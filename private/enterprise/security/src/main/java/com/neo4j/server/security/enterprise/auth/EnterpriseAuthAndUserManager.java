/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.UserManagerSupplier;

public interface EnterpriseAuthAndUserManager extends EnterpriseAuthManager, UserManagerSupplier
{
    @Override
    EnterpriseUserManager getUserManager( AuthSubject authSubject, boolean isUserManager );

    @Override
    EnterpriseUserManager getUserManager();
}
