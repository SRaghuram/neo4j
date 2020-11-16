/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.database.DefaultDatabaseResolver;

import static org.neo4j.kernel.api.security.AuthToken.invalidToken;

public class InClusterAuthManager extends EnterpriseAuthManager
{
    public static final String SCHEME = "in-cluster-token";
    public static final String ROLES_KEY = "roles";

    private final PrivilegeResolver privilegeResolver;
    private DefaultDatabaseResolver defaultDatabaseResolver;
    private final SecurityLog securityLog;
    private final boolean logSuccessfulLogin;

    public InClusterAuthManager( PrivilegeResolver privilegeResolver, DefaultDatabaseResolver defaultDatabaseResolver,
            SecurityLog securityLog, boolean logSuccessfulLogin )
    {
        this.privilegeResolver = privilegeResolver;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.securityLog = securityLog;
        this.logSuccessfulLogin = logSuccessfulLogin;
    }

    @Override
    public EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        try
        {
            assertValidScheme( authToken );
            String username = extractUsername( authToken );
            Set<String> roles = extractRoles( authToken );
            String defaultDatabaseForUser = defaultDatabaseResolver.defaultDatabase( username );

            var loginContext = new InClusterLoginContext( username, roles, defaultDatabaseForUser, () -> privilegeResolver.getPrivileges( roles ) );

            if ( logSuccessfulLogin )
            {
                securityLog.info( loginContext.subject(), "logged in within cluster" );
            }

            return loginContext;
        }
        catch ( InvalidAuthTokenException e )
        {
            securityLog.error( "Unknown user failed to log in: %s", e.getMessage() );
            throw e;
        }
    }

    @Override
    public void clearAuthCache()
    {
        // System Graph Realm is managed by the 'real' auth manger
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
        if ( !scheme.equals( SCHEME ) )
        {
            throw invalidToken( ", scheme '" + scheme + "' is not supported." );
        }
    }

    private String extractUsername( Map<String,Object> token ) throws InvalidAuthTokenException
    {
        return AuthToken.safeCast( AuthToken.PRINCIPAL, token );
    }

    private Set<String> extractRoles( Map<String,Object> token ) throws InvalidAuthTokenException
    {
        Map<String,Object> parameters = AuthToken.safeCastMap( AuthToken.PARAMETERS, token );
        var roles = parameters.get( ROLES_KEY );
        if ( roles == null )
        {
            throw new InvalidAuthTokenException( "'" + ROLES_KEY + "' parameter no provided" );
        }
        if ( !(roles instanceof List) )
        {
            throw new InvalidAuthTokenException( "The value associated with the parameter " + ROLES_KEY + "`' must be a List but was: "
                    + roles.getClass().getSimpleName() );
        }

        Set<String> result = new HashSet<>();

        for ( var role : (List) roles )
        {
            if ( !(role instanceof String) )
            {
                throw new InvalidAuthTokenException( "Role must be a String but was: " + role.getClass().getSimpleName() );
            }

            result.add( (String) role );
        }

        return result;
    }

    @Override
    public List<Map<String,String>> getPrivilegesGrantedThroughConfig()
    {
        return privilegeResolver.getPrivilegesGrantedThroughConfig();
    }
}
