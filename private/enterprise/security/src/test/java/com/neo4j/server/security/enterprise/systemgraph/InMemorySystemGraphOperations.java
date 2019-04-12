/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import org.neo4j.server.security.auth.SecureHasher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicInMemorySystemGraphOperations;
import org.neo4j.server.security.systemgraph.QueryExecutor;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.IS_SUSPENDED;
import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.assertValidRoleName;
import static org.mockito.Mockito.mock;
import static org.neo4j.server.security.systemgraph.BasicSystemGraphRealm.assertValidUsername;

public class InMemorySystemGraphOperations extends SystemGraphOperations
{
    private BasicInMemorySystemGraphOperations basic = new BasicInMemorySystemGraphOperations();
    private Map<String,Set<String>> rolesForUsers = new HashMap<>();
    private Map<String,RoleRecord> roles = new HashMap<>();
    private Map<String,Map<String,DatabasePrivilege>> rolePrivileges = new HashMap<>();

    public InMemorySystemGraphOperations( SecureHasher secureHasher )
    {
        super( mock( QueryExecutor.class ), secureHasher );
    }

    @Override
    public void addUser( User user ) throws InvalidArgumentsException
    {
        basic.addUser( user );
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return basic.getAllUsernames();
    }

    @Override
    AuthorizationInfo doGetAuthorizationInfo( String username )
    {
        User user = basic.users.get( username );
        if ( user == null || user.passwordChangeRequired() || user.hasFlag( IS_SUSPENDED ) )
        {
            return new SimpleAuthorizationInfo();
        }
        return new SimpleAuthorizationInfo( rolesForUsers.getOrDefault( username, Collections.emptySet() ) );
    }

    @Override
    void suspendUser( String username ) throws InvalidArgumentsException
    {
        User user = basic.users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        User augmented = user.augment().withFlag( IS_SUSPENDED ).build();
        basic.users.put( username, augmented );
    }

    @Override
    void activateUser( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        User user = basic.users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        User augmented = user.augment().withoutFlag( IS_SUSPENDED ).build();
        basic.users.put( username, augmented );
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
        removeRoleFromUsers( roleName, role.users() );
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
    void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException
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
    void removeRoleFromUser( String roleName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        getUser( username, false ); // This throws InvalidArgumentException if user does not exist
        assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
        RoleRecord roleRecord = roles.get( roleName );
        roles.put( roleName, roleRecord.augment().withoutUser( username ).build() );
        removeRoleFromUsers( roleName, Collections.singleton( username ) );
    }

    @Override
    void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege, String dbName ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName );
        Map<String,DatabasePrivilege> databasePrivilegeMap = rolePrivileges.computeIfAbsent( roleName, k -> new HashMap<>() );
        DatabasePrivilege dbPrivilege = databasePrivilegeMap.computeIfAbsent( dbName, DatabasePrivilege::new );
        dbPrivilege.addPrivilege( resourcePrivilege );
    }

    @Override
    void revokePrivilegeFromRole( String roleName, ResourcePrivilege resourcePrivilege, String dbName ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName );
        Map<String,DatabasePrivilege> databasePrivilegeMap = rolePrivileges.get( roleName );
        if ( databasePrivilegeMap != null )
        {
            DatabasePrivilege dbPrivilege = databasePrivilegeMap.get( dbName );
            if ( dbPrivilege != null )
            {
                dbPrivilege.removePrivilege( resourcePrivilege );
            }
        }
    }

    @Override
    Set<DatabasePrivilege> showPrivilegesForUser( String username ) throws InvalidArgumentsException
    {
        getUser( username, false );
        return getPrivilegeForRoles( getRoleNamesForUser( username ) );
    }

    @Override
    Set<DatabasePrivilege> getPrivilegeForRoles( Set<String> roles )
    {
        Map<String,DatabasePrivilege> privileges = new HashMap<>();
        for ( String role : roles )
        {
            Map<String,DatabasePrivilege> privilegeMap = rolePrivileges.get( role );
            if ( privilegeMap != null )
            {
                for ( DatabasePrivilege dbPrivilege : privilegeMap.values() )
                {
                    DatabasePrivilege oldPrivilege = privileges.get( dbPrivilege.getDbName() );
                    if ( oldPrivilege == null )
                    {
                        privileges.put( dbPrivilege.getDbName(), dbPrivilege );
                    }
                    else
                    {
                        for ( ResourcePrivilege privilege : dbPrivilege.getPrivileges() )
                        {
                            oldPrivilege.addPrivilege( privilege );
                        }
                    }
                }
            }
        }
        return new HashSet<>( privileges.values() );
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
    public boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        Set<String> rolesForUser = rolesForUsers.remove( username );
        if ( rolesForUser != null )
        {
            for ( String role : rolesForUser )
            {
                RoleRecord roleRecord = roles.get( role );
                roles.put( role, roleRecord.augment().withoutUser( username ).build() );
            }
        }

        return basic.deleteUser( username );
    }

    @Override
    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName );
        return roles.get( roleName ).users();
    }

    @Override
    public User getUser( String username, boolean silent ) throws InvalidArgumentsException
    {
        return basic.getUser( username, silent );
    }

    @Override
   protected void setUserCredentials( String username, String newCredentials, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        basic.setUserCredentials( username, newCredentials, requirePasswordChange );
    }

    private void removeRoleFromUsers( String roleName, Set<String> users )
    {
        for ( String user : users )
        {
            Set<String> roles = rolesForUsers.get( user );
            roles.remove( roleName );
        }
    }
}
