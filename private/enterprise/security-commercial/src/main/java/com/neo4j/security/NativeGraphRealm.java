/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.AllowAllCredentialsMatcher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.io.IOException;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import org.neo4j.server.security.enterprise.auth.RealmLifecycle;
import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
class NativeGraphRealm extends AuthorizingRealm implements RealmLifecycle, EnterpriseUserManager, ShiroAuthorizationInfoProvider
{
    private final UserRepository initialUserRepository;
    private final UserRepository defaultAdminRepository;
    private final PasswordPolicy passwordPolicy;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;
    private final boolean authorizationEnabled;

    NativeGraphRealm( PasswordPolicy passwordPolicy, AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled, boolean authorizationEnabled,
            UserRepository initialUserRepository, UserRepository defaultAdminRepository )
    {
        super();

        setName( SecuritySettings.NATIVE_REALM_NAME );
        this.initialUserRepository = initialUserRepository;
        this.defaultAdminRepository = defaultAdminRepository;
        this.passwordPolicy = passwordPolicy;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;
        this.authorizationEnabled = authorizationEnabled;
        setAuthenticationCachingEnabled( false );
        setAuthorizationCachingEnabled( false );
        setCredentialsMatcher( new AllowAllCredentialsMatcher() );
        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
    }

    @Override
    public void initialize() throws Throwable
    {
        initialUserRepository.init();
        defaultAdminRepository.init();
    }

    @Override
    public void start() throws Throwable
    {
        initialUserRepository.start();
        defaultAdminRepository.start();
    }

    @Override
    public void stop() throws Throwable
    {
        initialUserRepository.stop();
        defaultAdminRepository.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        initialUserRepository.shutdown();
        defaultAdminRepository.shutdown();
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        try
        {
            if ( token instanceof ShiroAuthToken )
            {
                ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
                return shiroAuthToken.getScheme().equals( AuthToken.BASIC_SCHEME ) &&
                        (shiroAuthToken.supportsRealm( AuthToken.NATIVE_REALM ));
            }
            return false;
        }
        catch ( InvalidAuthTokenException e )
        {
            return false;
        }
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        return null;
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        return null;
    }

    @Override
    public void suspendUser( String username ) throws IOException, InvalidArgumentsException
    {

    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange ) throws IOException, InvalidArgumentsException
    {

    }

    @Override
    public void newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
    {

    }

    @Override
    public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
    {
        return false;
    }

    @Override
    public void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {

    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {

    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {

    }

    @Override
    public Set<String> getAllRoleNames()
    {
        return null;
    }

    @Override
    public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        return null;
    }

    @Override
    public Set<String> silentlyGetRoleNamesForUser( String username )
    {
        return null;
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        return null;
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        return null;
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws IOException, InvalidArgumentsException
    {
        return null;
    }

    @Override
    public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
    {
        return false;
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        return null;
    }

    @Override
    public User silentlyGetUser( String username )
    {
        return null;
    }

    @Override
    public void setUserPassword( String username, String password, boolean requirePasswordChange ) throws IOException, InvalidArgumentsException
    {

    }

    @Override
    public Set<String> getAllUsernames()
    {
        return null;
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principal )
    {
        return null;
    }
}
