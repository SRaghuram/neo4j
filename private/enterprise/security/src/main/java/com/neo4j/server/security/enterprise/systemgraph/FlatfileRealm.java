/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.RealmLifecycle;
import com.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.ShiroAuthToken;
import org.neo4j.server.security.auth.UserRepository;

public class FlatfileRealm extends AuthorizingRealm implements RealmLifecycle, ShiroAuthorizationInfoProvider, CredentialsMatcher
{

    private static String REALM_NAME = "File";
    private AuthenticationStrategy authenticationStrategy;
    private String user;
    private UserRepository userRepository;

    public FlatfileRealm( AuthenticationStrategy authenticationStrategy, String user, UserRepository userRepository )
    {
        this.authenticationStrategy = authenticationStrategy;
        this.user = user;
        this.userRepository = userRepository;
        setName( REALM_NAME );
        setCredentialsMatcher( this );
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        if ( token instanceof ShiroAuthToken )
        {
            ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;

            try
            {
                AuthToken.safeCast( AuthToken.PRINCIPAL, shiroAuthToken.getAuthTokenMap() );
                AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
            }
            catch ( InvalidAuthTokenException e )
            {
                return false;
            }

            return shiroAuthToken.supportsRealm( getName() );
        }

        return false;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;

        String username = (String) shiroAuthToken.getPrincipal();

        User user = userRepository.getUserByName( username );
        if ( user == null )
        {
            throw new UnknownAccountException( );
        }

        return new SystemGraphAuthenticationInfo( user, getName() );
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principalCollection )
    {
        return null;
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principal )
    {
        return null;
    }

    @Override
    public void initialize() throws Exception
    {
        userRepository.init();
    }

    @Override
    public void start() throws Exception
    {
        userRepository.start();
    }

    @Override
    public void stop() throws Exception
    {
        userRepository.stop();
    }

    @Override
    public void shutdown() throws Exception
    {
        userRepository.shutdown();
    }

    @Override
    public boolean doCredentialsMatch( AuthenticationToken token, AuthenticationInfo info )
    {
        // We assume that the given info originated from this class, so we can get the user record from it
        SystemGraphAuthenticationInfo ourInfo = (SystemGraphAuthenticationInfo) info;
        User user = ourInfo.getUserRecord();

        // Get the password from the token
        byte[] password;
        try
        {
            ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
            password = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        // Authenticate using our strategy (i.e. with rate limiting)
        AuthenticationResult result = authenticationStrategy.authenticate( user, password );

        // Map failures to exceptions
        switch ( result )
        {
        case SUCCESS:
        case PASSWORD_CHANGE_REQUIRED:
            break;
        case FAILURE:
            throw new IncorrectCredentialsException();
        case TOO_MANY_ATTEMPTS:
            throw new ExcessiveAttemptsException();
        default:
            throw new AuthenticationException();
        }

        // Modify the given AuthenticationInfo with the final result and return with success.
        ourInfo.setAuthenticationResult( AuthenticationResult.SUCCESS );
        return true;
    }
}
