/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabaseSegment;
import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.subject.PrincipalCollection;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicInMemoryUserManager;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;
import org.neo4j.time.Clocks;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;

public class InMemoryUserManager extends SystemGraphRealm
{
    private BasicInMemoryUserManager basic;
    private Map<String,Set<String>> rolesForUsers = new HashMap<>();
    private Map<String,RoleRecord> roles = new HashMap<>();
    private Map<String,Set<ResourcePrivilege>> rolePrivileges = new HashMap<>();

    public InMemoryUserManager( Config config, SecureHasher secureHasher ) throws InvalidArgumentsException
    {
       this( config, secureHasher, new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config ) );
    }

    public InMemoryUserManager( Config config, SecureHasher secureHasher, AuthenticationStrategy authStrategy ) throws InvalidArgumentsException
    {
        super( null,
                SecurityGraphInitializer.NO_OP,
                secureHasher,
                new BasicPasswordPolicy(),
                authStrategy,
                true,
                true );
        basic = new BasicInMemoryUserManager( config );
        setupDefaultRolesAndPrivileges();
    }

    private void setupDefaultRolesAndPrivileges() throws InvalidArgumentsException
    {
        newRole( PredefinedRoles.READER );
        assignDefaultPrivileges( PredefinedRoles.READER );

        newRole( PredefinedRoles.EDITOR );
        assignDefaultPrivileges( PredefinedRoles.EDITOR );

        newRole( PredefinedRoles.PUBLISHER );
        assignDefaultPrivileges( PredefinedRoles.PUBLISHER );

        newRole( PredefinedRoles.ARCHITECT );
        assignDefaultPrivileges( PredefinedRoles.ARCHITECT );

        newRole( PredefinedRoles.ADMIN );
        assignDefaultPrivileges( PredefinedRoles.ADMIN );
        addRoleToUser( PredefinedRoles.ADMIN, INITIAL_USER_NAME );
    }

    // TODO this is almost a copy of EnterpriseSecurityGraphInitializer, consider to share code
    private void assignDefaultPrivileges( String roleName ) throws InvalidArgumentsException
    {
        if ( PredefinedRolesBuilder.roles.containsKey( roleName ) )
        {
            SimpleRole simpleRole = PredefinedRolesBuilder.roles.get( roleName );
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.SYSTEM ) )
            {
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.ADMIN, new Resource.DatabaseResource(), DatabaseSegment.ALL ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.SCHEMA ) )
            {
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.SCHEMA, new Resource.DatabaseResource(), DatabaseSegment.ALL ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.TOKEN ) )
            {
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.TOKEN, new Resource.DatabaseResource(), DatabaseSegment.ALL ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.WRITE ) )
            {
                // The segment part is ignored for this action
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.WRITE, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
               grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.WRITE, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.READ ) )
            {
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL ) );
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL ) );
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.READ, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.ACCESS ) )
            {
                grantPrivilegeToRole( roleName,
                        new ResourcePrivilege( GRANT, PrivilegeAction.ACCESS, new Resource.DatabaseResource(), DatabaseSegment.ALL ) );
            }
        }
    }

    @Override
    public User newUser( String username, byte[] initialPassword, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        return basic.newUser( username, initialPassword, requirePasswordChange );
    }

    @Override
    public void setUserPassword( String username, byte[] password, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        basic.setUserPassword( username, password, requirePasswordChange );
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return basic.getAllUsernames();
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        return basic.getUser( username );
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
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        String username = (String) getAvailablePrincipal( principals );
        if ( username == null )
        {
            return null;
        }

        return doGetAuthorizationInfo( username );
    }

    private AuthorizationInfo doGetAuthorizationInfo( String username )
    {
        User user = basic.users.get( username );
        if ( user == null || user.passwordChangeRequired() || user.hasFlag( IS_SUSPENDED ) )
        {
            return new SimpleAuthorizationInfo();
        }
        return new SimpleAuthorizationInfo( rolesForUsers.getOrDefault( username, Collections.emptySet() ) );
    }

    @Override
    public void newRole( String roleName, String... usernames ) throws InvalidArgumentsException
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
    public boolean deleteRole( String roleName ) throws InvalidArgumentsException
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
    public void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        if ( !roles.containsKey( roleName ) )
        {
            throw new InvalidArgumentsException( "Role '" + roleName + "' does not exist." );
        }
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        getUser( username); // This throws InvalidArgumentException if user does not exist
        assertRoleExists( roleName ); //This throws InvalidArgumentException if role does not exist
        RoleRecord role = roles.get( roleName );
        RoleRecord augmented = role.augment().withUser( username ).build();
        roles.put( roleName, augmented );
        Set<String> rolesForUser = rolesForUsers.computeIfAbsent( username, k -> new HashSet<>() );
        rolesForUser.add( roleName );
    }

    private void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName );
        Set<ResourcePrivilege> privilegesForRole = rolePrivileges.computeIfAbsent( roleName, k -> new HashSet<>() );
        privilegesForRole.add( resourcePrivilege );
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
    {
        Set<ResourcePrivilege> privileges = new HashSet<>();
        for ( String role : roles )
        {
            Set<ResourcePrivilege> privilegeForRole = rolePrivileges.get( role );
            if ( privilegeForRole != null )
            {
                privileges.addAll( privilegeForRole );
            }
        }
        return privileges;
    }

    @Override
    public Set<String> getAllRoleNames()
    {
        return roles.keySet();
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName );
        return roles.get( roleName ).users();
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
