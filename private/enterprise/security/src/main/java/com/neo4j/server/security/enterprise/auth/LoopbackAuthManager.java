/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.pam.UnsupportedTokenException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.ShiroAuthToken;
import org.neo4j.server.security.auth.UserRepository;

import static org.neo4j.kernel.api.security.AuthToken.invalidToken;
import static org.neo4j.server.security.systemgraph.SystemGraphRealmHelper.IS_SUSPENDED;

public class LoopbackAuthManager extends EnterpriseAuthManager
{
    public static final String SCHEME = "in-cluster-token";

    private final SecurityLog securityLog;
    private final boolean logSuccessfulLogin;
    private final AuthenticationStrategy authenticationStrategy;
    private final UserRepository userRepository;
    private User user;

    public LoopbackAuthManager( AuthenticationStrategy authenticationStrategy,
                                UserRepository userRepository,
                                SecurityLog securityLog,
                                boolean logSuccessfulLogin )
    {
        this.authenticationStrategy = authenticationStrategy;
        this.userRepository = userRepository;
        this.securityLog = securityLog;
        this.logSuccessfulLogin = logSuccessfulLogin;
    }

    @Override
    public EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        ShiroAuthToken token = new ShiroAuthToken( authToken );
        try
        {
            assertValidScheme( authToken );

            // Get the password from the token
            byte[] password;
            try
            {
                password = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, token.getAuthTokenMap() );
            }
            catch ( InvalidAuthTokenException e )
            {
                throw new UnsupportedTokenException( e );
            }

            // Authenticate using our strategy (i.e. with rate limiting)
            AuthenticationResult result = authenticationStrategy.authenticate( user, password );

            // log result
            switch ( result )
            {
            case SUCCESS:
                if ( logSuccessfulLogin )
                {
                    securityLog.info( "Operator logged in" );
                }
                break;
            case PASSWORD_CHANGE_REQUIRED:
                // this should not be possible since the credentials are explicitly set to not be expired.
                securityLog.error( "Operator user logged in with expired credentials" );
                break;
            case FAILURE:
                securityLog.error( "Operator user failed to log in" );
                break;
            case TOO_MANY_ATTEMPTS:
                securityLog.error( "Operator user failed to log in: too many failed attempts" );
                break;
            default:
                throw new AuthenticationException();
            }
            return new LoopbackLoginContext( result );
        }
        catch ( InvalidAuthTokenException e )
        {
            securityLog.error( "Unknown user failed to log in: %s", e.getMessage() );
            throw invalidToken( ": " + token );
        }
    }

    @Override
    public void clearAuthCache()
    {
        // No cache used here
    }

    @Override
    public void log( String message, SecurityContext securityContext )
    {
        securityLog.info( securityContext.subject(), message );
    }

    private void assertValidScheme( Map<String,Object> token ) throws InvalidAuthTokenException
    {
        String scheme = AuthToken.safeCast( AuthToken.SCHEME_KEY, token );
        if ( scheme.equals( "none" ) )
        {
            throw invalidToken( ", scheme 'none' is only allowed when auth is disabled." );
        }
        if ( !scheme.equals( AuthToken.BASIC_SCHEME ) )
        {
            throw invalidToken( ", scheme '" + scheme + "' is not supported." );
        }
    }

    @Override
    public List<Map<String,String>> getPrivilegesGrantedThroughConfig()
    {
        return Collections.emptyList();
    }

    @Override
    public void start() throws Exception
    {
        // extract the operator user and ensure it is the only one in the file
        userRepository.start();
        List<User> users = userRepository.getSnapshot().values();
        if ( users.size() != 1 )
        {
            throw new IllegalStateException(
                    "No password has been set for the loopback operator. Run `neo4j-admin set-operator-password <password>`." );
        }
        else
        {
            user = users.get( 0 ).augment().withRequiredPasswordChange( false ).withoutFlag( IS_SUSPENDED ).build();
        }
    }

    private static class LoopbackLoginContext implements EnterpriseLoginContext
    {
        private final AuthenticationResult authResult;

        LoopbackLoginContext( AuthenticationResult authResult )
        {
            this.authResult = authResult;
        }

        @Override
        public Set<String> roles()
        {
            return Collections.emptySet();
        }

        @Override
        public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
        {
            if ( !(authResult.equals( AuthenticationResult.SUCCESS )) )
            {
                throw new AuthorizationViolationException( AuthorizationViolationException.PERMISSION_DENIED, Status.Security.Unauthorized );
            }
            return EnterpriseSecurityContext.AUTH_DISABLED;
        }

        @Override
        public AuthSubject subject()
        {
            return LOOPBACK_SUBJECT;
        }
    }

    private static final AuthSubject LOOPBACK_SUBJECT = new AuthSubject()
    {
        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return false;
        }

        @Override
        public String username()
        {
            return "LOOPBACK_OPERATOR";
        }
    };
}
