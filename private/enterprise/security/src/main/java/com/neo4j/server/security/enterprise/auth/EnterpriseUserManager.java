/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Arrays;
import java.util.Set;

import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;

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

        @Override
        public User newUser( String username, byte[] initialPassword, boolean requirePasswordChange )
        {
            if ( initialPassword != null )
            {
                Arrays.fill( initialPassword, (byte) 0 );
            }
            return null;
        }

        @Override
        public User getUser( String username )
        {
            return null;
        }

        @Override
        public User silentlyGetUser( String username )
        {
            return null;
        }

        @Override
        public void setUserPassword( String username, byte[] password, boolean requirePasswordChange )
        {
            if ( password != null )
            {
                Arrays.fill( password, (byte) 0 );
            }
        }
    };
}
