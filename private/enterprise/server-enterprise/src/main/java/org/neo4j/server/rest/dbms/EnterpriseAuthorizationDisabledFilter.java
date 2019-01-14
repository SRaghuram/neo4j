/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest.dbms;

import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;

public class EnterpriseAuthorizationDisabledFilter extends AuthorizationDisabledFilter
{
    @Override
    protected EnterpriseLoginContext getAuthDisabledLoginContext()
    {
        return EnterpriseLoginContext.AUTH_DISABLED;
    }
}
