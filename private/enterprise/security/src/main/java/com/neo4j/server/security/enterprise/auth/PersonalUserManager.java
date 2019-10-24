/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

class PersonalUserManager implements EnterpriseUserManager
{
    private final EnterpriseUserManager userManager;
    private final boolean isUserManager;

    PersonalUserManager( EnterpriseUserManager userManager, boolean isUserManager )
    {
        this.userManager = userManager;
        this.isUserManager = isUserManager;
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
    {
        assertUserManager();
        return userManager.getPrivilegesForRoles( roles );
    }

    @Override
    public void clearCacheForRoles()
    {
        userManager.clearCacheForRoles();
    }

    private void assertUserManager() throws AuthorizationViolationException
    {
        if ( !isUserManager )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }
}
