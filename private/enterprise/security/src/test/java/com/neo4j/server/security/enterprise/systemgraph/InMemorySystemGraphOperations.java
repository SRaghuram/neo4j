/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.SecureHasher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.LegacyCredential;

import static org.mockito.Mockito.mock;

public class InMemorySystemGraphOperations extends SystemGraphOperations
{
    private static final String SUSPENDED = "is_suspended";

    private Map<String,User> users = new HashMap<>();
    private Map<String,Set<String>> rolesForUsers = new HashMap<>();
    private Map<String,RoleRecord> roles = new HashMap<>();

    public InMemorySystemGraphOperations( SecureHasher secureHasher )
    {
        super( mock( QueryExecutor.class ), secureHasher );
        for ( String roleName : PredefinedRolesBuilder.roles.keySet() )
        {
            roles.put( roleName, new RoleRecord( roleName ) );
        }
    }

    @Override
    void addUser( User user ) throws InvalidArgumentsException
    {
        String username = user.name();
        assertValidUsername( username );
        if ( users.containsKey( username ) )
        {
            throw new InvalidArgumentsException( "The specified user '" + username + "' already exists." );
        }
        users.put( username, user );
    }

    @Override
    Set<String> getAllUsernames()
    {
        return users.keySet();
    }

    @Override
    AuthorizationInfo doGetAuthorizationInfo( String username )
    {
        User user = users.get( username );
        if ( user == null || user.passwordChangeRequired() || user.hasFlag( SUSPENDED ) )
        {
            return new SimpleAuthorizationInfo();
        }
        return new SimpleAuthorizationInfo( rolesForUsers.getOrDefault( username, Collections.emptySet() ) );
    }

    @Override
    void suspendUser( String username ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        User augmented = user.augment().withFlag( SUSPENDED ).build();
        users.put( username, augmented );
    }

    @Override
    void activateUser( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        User augmented = user.augment().withoutFlag( SUSPENDED ).build();
        users.put( username, augmented );
    }

    @Override
    void newRole( String roleName, String... usernames ) throws InvalidArgumentsException
    {
        if ( roles.containsKey( roleName ) )
        {
            throw new InvalidArgumentsException( "The specified role '" + roleName + "' already exists." );
        }
        RoleRecord role = new RoleRecord( roleName );
        roles.put( roleName, role );
        for ( String username : usernames )
        {
            addRoleToUser( roleName, username );
        }
    }

    @Override
    boolean deleteRole( String roleName ) throws InvalidArgumentsException
    {
        RoleRecord role = roles.get( roleName );
        if ( role == null )
        {
            throw new InvalidArgumentsException( "Role '" + roleName + "' does not exist." );
        }
        roles.remove( roleName );
        return true;
    }

    @Override
    void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        if ( !roles.containsKey( roleName ) )
        {
            throw new InvalidArgumentsException( "Role '" + roleName + "' does not exist." );
        }
    }

    @Override
    void addRoleToUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        getUser( username, false ); // This throws InvalidArgumentException if user does not exist
        assertRoleExists( roleName ); //This throws InvalidArgumentException if role does not exist
        RoleRecord role = roles.get( roleName );
        RoleRecord augmented = role.augment().withUser( username ).build();
        roles.put( roleName, augmented );
        Set<String> rolesForUser = rolesForUsers.computeIfAbsent( username, k -> new HashSet<>() );
        rolesForUser.add( roleName );
    }

    @Override
    void removeRoleFromUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        getUser( username, false ); // This throws InvalidArgumentException if user does not exist
        assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
        RoleRecord roleRecord = roles.get( roleName );
        roles.put( roleName, roleRecord.augment().withoutUser( username ).build() );
    }

    @Override
    Set<String> getAllRoleNames()
    {
        return roles.keySet();
    }

    @Override
    Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        getUser( username, false );
        return rolesForUsers.get( username );
    }

    @Override
    boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        User removed = users.remove( username );
        if ( removed == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        Set<String> rolesForUser = rolesForUsers.remove( username );
        if ( rolesForUser != null )
        {
            for ( String role : rolesForUser )
            {
                RoleRecord roleRecord = roles.get( role );
                roles.put( role, roleRecord.augment().withoutUser( username ).build() );
            }
        }
        return true;
    }

    @Override
    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName );
        return roles.get( roleName ).users();
    }

    @Override
    User getUser( String username, boolean silent ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( !silent && user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return user;
    }

    @Override
    void setUserCredentials( String username, String newCredentials, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        User augmented = user.augment()
                .withCredentials( LegacyCredential.forPassword( newCredentials ) )
                .withRequiredPasswordChange( requirePasswordChange ).build();
        users.put( username, augmented );
    }

    @Override
    Set<String> getDbNamesForUser( String username )
    {
        throw new UnsupportedOperationException( "getDbNamesForUser not implemented for this stub" );
    }
}
