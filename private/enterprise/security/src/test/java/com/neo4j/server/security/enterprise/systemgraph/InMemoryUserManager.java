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
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
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

    public InMemoryUserManager( Config config ) throws InvalidArgumentsException
    {
       this( config, new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config ) );
    }

    public InMemoryUserManager( Config config, AuthenticationStrategy authStrategy ) throws InvalidArgumentsException
    {
        super( SecurityGraphInitializer.NO_OP, null, new SecureHasher(), authStrategy, true, true );
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

    public void newUser( String username, byte[] initialPassword, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        basic.newUser( username, initialPassword, requirePasswordChange );
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        return basic.getUser( username );
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        String username = (String) getAvailablePrincipal( principals );
        if ( username == null )
        {
            return null;
        }

        User user = basic.users.get( username );
        if ( user == null || user.passwordChangeRequired() || user.hasFlag( IS_SUSPENDED ) )
        {
            return new SimpleAuthorizationInfo();
        }
        return new SimpleAuthorizationInfo( rolesForUsers.getOrDefault( username, Collections.emptySet() ) );
    }

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

    private void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        if ( !roles.containsKey( roleName ) )
        {
            throw new InvalidArgumentsException( "Role '" + roleName + "' does not exist." );
        }
    }

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

    private void assertValidRoleName( String name ) throws InvalidArgumentsException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided role name is empty." );
        }
        if ( !roleNamePattern.matcher( name ).matches() )
        {
            throw new InvalidArgumentsException( "Role name '" + name + "' contains illegal characters. Use simple ascii characters and numbers." );
        }
    }

    private static final Pattern roleNamePattern = Pattern.compile( "^[a-zA-Z0-9_]+$" );
}
