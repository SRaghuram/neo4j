/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;

import static java.util.Collections.emptySet;

public interface EnterpriseUserManager extends UserManager
{
    void suspendUser( String username ) throws IOException, InvalidArgumentsException;

    void activateUser( String username, boolean requirePasswordChange ) throws IOException, InvalidArgumentsException;

    void newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException;

    void newCopyOfRole( String roleName, String from ) throws IOException, InvalidArgumentsException;

    boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException;

    void assertRoleExists( String roleName ) throws InvalidArgumentsException;

    /**
     * Assign a role to a user. The role and the user have to exist.
     *
     * @param roleName name of role
     * @param username name of user
     * @throws InvalidArgumentsException if the role does not exist
     * @throws IOException
     */
    void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException;

    /**
     * Unassign a role from a user. The role and the user have to exist.
     *
     * @param roleName name of role
     * @param username name of user
     * @throws InvalidArgumentsException if the username or the role does not exist
     * @throws IOException
     */
    void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException;

    /**
     * Grant privilege on a resource to a role. The role have to exist.
     * If the role already have this privilege nothing will change.
     *
     * @param roleName name of role
     * @param privilege privilege to grant
     * @throws InvalidArgumentsException if the role or database does not exist
     */
    void grantPrivilegeToRole( String roleName, ResourcePrivilege privilege ) throws InvalidArgumentsException;

    /**
     * Revoke a privilege on a resource from a role. The role have to exist.
     * If the role does not have a existing grant on the privilege, no change will happen.
     *
     * @param roleName name of role
     * @param privilege privilege to revoke
     * @throws InvalidArgumentsException if the role or database does not exist
     */
    void revokePrivilegeFromRole( String roleName, ResourcePrivilege privilege ) throws InvalidArgumentsException;

    /**
     * Show the privileges for a user.
     *
     * @param username name of user
     * @throws InvalidArgumentsException if the user does not exist
     */
    Set<ResourcePrivilege> showPrivilegesForUser( String username ) throws InvalidArgumentsException;

    Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles );

    /**
     * Clear the cached privileges for a role.
     * @param role name of the role
     */
    void clearCacheForRole( String role );

    Set<String> getAllRoleNames();

    Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException;

    Set<String> silentlyGetRoleNamesForUser( String username );

    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException;

    Set<String> silentlyGetUsernamesForRole( String roleName );

    EnterpriseUserManager NOOP = new EnterpriseUserManager()
    {
        @Override
        public void suspendUser( String username )
        {
        }

        @Override
        public void activateUser( String username, boolean requirePasswordChange )
        {
        }

        @Override
        public void newRole( String roleName, String... usernames )
        {
        }

        @Override
        public void newCopyOfRole( String roleName, String from )
        {
        }

        @Override
        public boolean deleteRole( String roleName )
        {
            return false;
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
        public void removeRoleFromUser( String roleName, String username )
        {
        }

        @Override
        public void grantPrivilegeToRole( String roleName, ResourcePrivilege privilege )
        {
        }

        @Override
        public void revokePrivilegeFromRole( String roleName, ResourcePrivilege privilege )
        {
        }

        @Override
        public Set<ResourcePrivilege> showPrivilegesForUser( String username )
        {
            return emptySet();
        }

        @Override
        public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
        {
            return emptySet();
        }

        @Override
        public void clearCacheForRole( String role )
        {
        }

        @Override
        public Set<String> getAllRoleNames()
        {
            return emptySet();
        }

        @Override
        public Set<String> getRoleNamesForUser( String username )
        {
            return emptySet();
        }

        @Override
        public Set<String> silentlyGetRoleNamesForUser( String username )
        {
            return emptySet();
        }

        @Override
        public Set<String> getUsernamesForRole( String roleName )
        {
            return emptySet();
        }

        @Override
        public Set<String> silentlyGetUsernamesForRole( String roleName )
        {
            return emptySet();
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
        public boolean deleteUser( String username )
        {
            return false;
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

        @Override
        public void setUserRequirePasswordChange( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
        {
        }

        @Override
        public void setUserStatus( String username, boolean isSuspended ) throws InvalidArgumentsException
        {
        }

        @Override
        public Set<String> getAllUsernames()
        {
            return emptySet();
        }
    };
}
