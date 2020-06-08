/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.neo4j.kernel.api.security.AuthToken.invalidToken;

public class InClusterAuthManager implements EnterpriseAuthManager
{
    private static final String SCHEME = "in-cluster-token";
    private static final String ROLES_KEY = "roles";

    private final SystemGraphRealm systemGraphRealm;

    private final SecurityLog securityLog;
    private final boolean logSuccessfulLogin;
    private final String defaultDatabase;

    public InClusterAuthManager( SystemGraphRealm systemGraphRealm,
            SecurityLog securityLog, boolean logSuccessfulLogin, String defaultDatabase )
    {
        this.systemGraphRealm = systemGraphRealm;
        this.securityLog = securityLog;
        this.logSuccessfulLogin = logSuccessfulLogin;
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        try
        {
            assertValidScheme( authToken );
            String username = extractUsername( authToken );
            Set<String> roles = extractRoles( authToken );

            var loginContext = new InClusterLoginContext( username, roles, defaultDatabase, () -> systemGraphRealm.getPrivilegesForRoles( roles ) );

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

    @Override
    public void init()
    {
        // System Graph Realm is managed by the 'real' auth manger
    }

    @Override
    public void start()
    {

    }

    @Override
    public void stop()
    {

    }

    @Override
    public void shutdown()
    {

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
}
