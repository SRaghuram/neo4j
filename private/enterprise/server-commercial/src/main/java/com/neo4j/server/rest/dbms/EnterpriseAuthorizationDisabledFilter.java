/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.dbms;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;

import org.neo4j.server.rest.dbms.AuthorizationDisabledFilter;

public class EnterpriseAuthorizationDisabledFilter extends AuthorizationDisabledFilter
{
    @Override
    protected CommercialLoginContext getAuthDisabledLoginContext()
    {
        return CommercialLoginContext.AUTH_DISABLED;
    }
}
