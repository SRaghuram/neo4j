/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Arrays;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;

import static java.util.Collections.emptySet;

public interface EnterpriseUserManager extends UserManager
{
    void newRole( String roleName, String... usernames ) throws InvalidArgumentsException;

    void assertRoleExists( String roleName ) throws InvalidArgumentsException;

    /**
     * Assign a role to a user. The role and the user have to exist.
     *
     * @param roleName name of role
     * @param username name of user
     * @throws InvalidArgumentsException if the role does not exist
     */
    void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException;

    Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles );

    /**
     * Clear the cached privileges for all roles.
     */
    void clearCacheForRoles();

    EnterpriseUserManager NOOP = new EnterpriseUserManager()
    {

        @Override
        public void newRole( String roleName, String... usernames )
        {
        }

        @Override
        public void assertRoleExists( String roleName )
        {
        }

        @Override
        public void addRoleToUser( String roleName, String username )
        {
        }

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
