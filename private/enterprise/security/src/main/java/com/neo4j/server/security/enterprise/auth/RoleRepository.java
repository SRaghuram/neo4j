/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * A component that can store and retrieve roles. Implementations must be thread safe.
 */
public interface RoleRepository extends Lifecycle
{
    RoleRecord getRoleByName( String roleName );

    Set<String> getRoleNamesByUsername( String username );

    /**
     * Clears all cached role data.
     */
    void clear();

    /**
     * Create a role, given that the roles token is unique.
     *
     * @param role the new role object
     * @throws InvalidArgumentsException if the role name is not valid or the role name already exists
     */
    void create( RoleRecord role ) throws InvalidArgumentsException, IOException;

    /**
     * Replaces the roles in the repository with the given roles.
     * @param roles the new roles
     * @throws InvalidArgumentsException if any role name is not valid
     * @throws IOException if the underlying storage for roles fails
     */
    void setRoles( ListSnapshot<RoleRecord> roles ) throws InvalidArgumentsException;

    /**
     * Update a role, given that the role token is unique.
     *
     * @param existingRole the existing role object, which must match the current state in this repository
     * @param updatedRole the updated role object
     * @throws ConcurrentModificationException if the existingRole does not match the current state in the repository
     */
    void update( RoleRecord existingRole, RoleRecord updatedRole )
            throws ConcurrentModificationException, IOException;

    /**
     * Deletes a role.
     *
     * @param role the role to delete
     * @return true if the role was found and deleted
     */
    boolean delete( RoleRecord role ) throws IOException;

    int numberOfRoles();

    /**
     * Asserts whether the given role name is valid or not. A valid role name is non-null, non-empty, and contains
     * only simple ascii characters.
     * @param roleName the role name to be tested.
     * @throws InvalidArgumentsException if the role name was invalid.
     */
    void assertValidRoleName( String roleName ) throws InvalidArgumentsException;

    void removeUserFromAllRoles( String username )
            throws ConcurrentModificationException, IOException;

    Set<String> getAllRoleNames();

    /**
     * Returns a snapshot of the current persisted role repository
     * @return a snapshot of the current persisted role repository
     * @throws IOException
     */
    ListSnapshot<RoleRecord> getPersistedSnapshot() throws IOException;

    /**
     * Permanently deletes all data in this repository
     * @throws IOException
     */
    void purge() throws IOException;

    /**
     * Mark this repository as migrated to prevent accidental use.
     * @throws IOException
     */
    void markAsMigrated() throws IOException;

    static boolean validate( List<User> users, List<RoleRecord> roles )
    {
        Set<String> usernamesInRoles = roles.stream()
                .flatMap( rr -> rr.users().stream() )
                .collect( Collectors.toSet() );
        Set<String> usernameInUsers = users.stream().map( User::name ).collect( Collectors.toSet() );
        return usernameInUsers.containsAll( usernamesInRoles );
    }
}
