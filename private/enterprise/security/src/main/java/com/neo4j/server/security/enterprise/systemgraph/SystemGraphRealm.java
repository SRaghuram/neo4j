/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import com.neo4j.server.security.enterprise.auth.RealmLifecycle;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;

import static java.lang.String.format;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
public class SystemGraphRealm extends BasicSystemGraphRealm implements RealmLifecycle, EnterpriseUserManager, ShiroAuthorizationInfoProvider
{
    private final boolean authorizationEnabled;
    private final SystemGraphOperations systemGraphOperations;

    public SystemGraphRealm( SystemGraphOperations systemGraphOperations, SecurityGraphInitializer systemGraphInitializer,
            SecureHasher secureHasher, PasswordPolicy passwordPolicy, AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled,
            boolean authorizationEnabled )
    {
        super( systemGraphOperations, systemGraphInitializer, secureHasher, passwordPolicy, authenticationStrategy, authenticationEnabled );
        setName( SecuritySettings.NATIVE_REALM_NAME );
        this.authorizationEnabled = authorizationEnabled;
        this.systemGraphOperations = systemGraphOperations;

        setAuthorizationCachingEnabled( true );
    }

    @Override
    public void initialize()
    {
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        if ( !authorizationEnabled )
        {
            return null;
        }

        String username = (String) getAvailablePrincipal( principals );
        if ( username == null )
        {
            return null;
        }

        return systemGraphOperations.doGetAuthorizationInfo( username );
    }

    @Override
    protected Object getAuthorizationCacheKey( PrincipalCollection principals )
    {
        return getAvailablePrincipal( principals );
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principalCollection )
    {
        return getAuthorizationInfo( principalCollection );
    }

    @Override
    public void suspendUser( String username ) throws InvalidArgumentsException
    {
        systemGraphOperations.suspendUser( username );
        clearCacheForUser( username );
    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        systemGraphOperations.activateUser( username, requirePasswordChange );
        clearCacheForUser( username );
    }

    @Override
    public void newRole( String roleName, String... usernames ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        systemGraphOperations.newRole( roleName, usernames );
    }

    @Override
    public void newCopyOfRole( String roleName, String from ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        systemGraphOperations.newCopyOfRole( roleName, from );
    }

    @Override
    public boolean deleteRole( String roleName ) throws InvalidArgumentsException
    {
        assertNotPredefinedRoleName( roleName );
        boolean success = systemGraphOperations.deleteRole( roleName );
        clearCachedAuthorizationInfo();
        return success;
    }

    @Override
    public void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        systemGraphOperations.assertRoleExists( roleName );
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException
    {
        systemGraphOperations.addRoleToUser( roleName, username );
        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws InvalidArgumentsException
    {
        systemGraphOperations.removeRoleFromUser( roleName, username );
        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public void grantPrivilegeToRole( String roleName, DatabasePrivilege dbPrivilege ) throws InvalidArgumentsException
    {
        for ( ResourcePrivilege privilege : dbPrivilege.getPrivileges() )
        {
            systemGraphOperations.grantPrivilegeToRole( roleName, privilege, dbPrivilege );
        }
        clearCachedAuthorizationInfo();
    }

    @Override
    public void revokePrivilegeFromRole( String roleName, DatabasePrivilege dbPrivilege ) throws InvalidArgumentsException
    {
        for ( ResourcePrivilege privilege : dbPrivilege.getPrivileges() )
        {
            systemGraphOperations.revokePrivilegeFromRole( roleName, privilege, dbPrivilege );
        }
        clearCachedAuthorizationInfo();
    }

    @Override
    public Set<DatabasePrivilege> showPrivilegesForUser( String username ) throws InvalidArgumentsException
    {
        return systemGraphOperations.showPrivilegesForUser( username );
    }

    @Override
    public Set<DatabasePrivilege> getPrivilegesForRoles( Set<String> roles )
    {
        return systemGraphOperations.getPrivilegeForRoles( roles );
    }

    @Override
    public Set<String> getAllRoleNames()
    {
        return systemGraphOperations.getAllRoleNames();
    }

    @Override
    public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        return systemGraphOperations.getRoleNamesForUser( username );
    }

    @Override
    public Set<String> silentlyGetRoleNamesForUser( String username )
    {
        try
        {
            return getRoleNamesForUser( username );
        }
        catch ( InvalidArgumentsException e )
        {
            return Collections.emptySet();
        }
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        return systemGraphOperations.getUsernamesForRole( roleName );
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        try
        {
            return getUsernamesForRole( roleName );
        }
        catch ( InvalidArgumentsException e )
        {
            return Collections.emptySet();
        }
    }

    private static void assertNotPredefinedRoleName( String roleName ) throws InvalidArgumentsException
    {
        if ( roleName != null && PredefinedRolesBuilder.roles.keySet().contains( roleName ) )
        {
            throw new InvalidArgumentsException(
                    format( "'%s' is a predefined role and can not be deleted or modified.", roleName ) );
        }
    }

    private static final Pattern roleNamePattern = Pattern.compile( "^[a-zA-Z0-9_]+$" );

    static void assertValidRoleName( String name ) throws InvalidArgumentsException
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

    private void clearCachedAuthorizationInfoForUser( String username )
    {
        clearCachedAuthorizationInfo( new SimplePrincipalCollection( username, this.getName() ) );
    }

    private void clearCachedAuthorizationInfo()
    {
        Cache<Object, AuthorizationInfo> cache = getAuthorizationCache();
        if ( cache != null )
        {
            cache.clear();
        }
    }
}
