/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Set;

import org.neo4j.kernel.api.security.UserManager;

import static java.util.Collections.emptySet;

public interface EnterpriseUserManager extends UserManager
{
    Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles );

    /**
     * Clear the cached privileges for all roles.
     */
    void clearCacheForRoles();

    EnterpriseUserManager NOOP = new EnterpriseUserManager()
    {
        @Override
        public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
        {
            return emptySet();
        }

        @Override
        public void clearCacheForRoles()
        {
        }
    };
}
