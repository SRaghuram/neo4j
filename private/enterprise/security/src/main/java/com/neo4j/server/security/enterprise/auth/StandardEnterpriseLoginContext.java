/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.apache.shiro.authz.AuthorizationInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class StandardEnterpriseLoginContext implements EnterpriseLoginContext
{
    private final MultiRealmAuthManager authManager;
    private final ShiroSubject shiroSubject;
    private final String defaultDatabase;
    private final NeoShiroSubject neoShiroSubject;

    StandardEnterpriseLoginContext( MultiRealmAuthManager authManager, ShiroSubject shiroSubject, String defaultDatabase )
    {
        this.authManager = authManager;
        this.shiroSubject = shiroSubject;
        this.defaultDatabase = defaultDatabase;
        this.neoShiroSubject = new NeoShiroSubject();
    }

    @Override
    public AuthSubject subject()
    {
        return neoShiroSubject;
    }

    private StandardAccessMode mode( IdLookup resolver, String dbName, String username )
    {
        boolean isAuthenticated = shiroSubject.isAuthenticated();
        boolean passwordChangeRequired = shiroSubject.getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        Set<String> roles = queryForRoleNames();
        StandardAccessModeBuilder accessModeBuilder =
                new StandardAccessModeBuilder( isAuthenticated, passwordChangeRequired, roles, resolver, dbName, defaultDatabase );

        Set<ResourcePrivilege> privileges = authManager.getPermissions( roles, username );
        return mode( accessModeBuilder, privileges, dbName, defaultDatabase, neoShiroSubject.username(), roles );
    }

    static StandardAccessMode mode( StandardAccessModeBuilder accessModeBuilder, Set<ResourcePrivilege> privileges, String dbName, String defaultDatabase,
            String username, Set<String> roles )
    {
        boolean isDefault = dbName.equals( defaultDatabase );
        for ( ResourcePrivilege privilege : privileges )
        {
            if ( privilege.appliesTo( dbName ) || isDefault && privilege.appliesToDefault() )
            {
                accessModeBuilder.addPrivilege( privilege );
            }
        }
        if ( dbName.equals( SYSTEM_DATABASE_NAME ) )
        {
            accessModeBuilder.withAccess();
        }

        StandardAccessMode mode = accessModeBuilder.build();
        if ( !mode.allowsAccess() )
        {
            throw mode.onViolation(
                String.format( "Database access is not allowed for user '%s' with roles %s.", username, new TreeSet<>( roles ).toString() ) );
        }

        return mode;
    }

    @Override
    public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
    {
        if ( !shiroSubject.isAuthenticated() )
        {
            throw new AuthorizationViolationException( AuthorizationViolationException.PERMISSION_DENIED, Status.Security.Unauthorized );
        }
        else if ( !dbName.equals( SYSTEM_DATABASE_NAME ) && shiroSubject.getAuthenticationResult().equals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) )
        {
            throw AccessMode.Static.CREDENTIALS_EXPIRED.onViolation( "Permission denied." );
        }
        StandardAccessMode mode = mode( idLookup, dbName, neoShiroSubject.username() );
        return new EnterpriseSecurityContext( neoShiroSubject, mode, mode.roles(), mode.getAdminAccessMode() );
    }

    @Override
    public Set<String> roles()
    {
        return queryForRoleNames();
    }

    private Set<String> queryForRoleNames()
    {
        Collection<AuthorizationInfo> authorizationInfo =
                authManager.getAuthorizationInfo( shiroSubject.getPrincipals() );
        Set<String> collection = authorizationInfo.stream().flatMap( authInfo -> {
            Collection<String> roles = authInfo.getRoles();
            return roles == null ? Stream.empty() : roles.stream();
        } ).collect( Collectors.toSet() );
        if ( authManager.shouldGetPublicRole( shiroSubject.getPrincipal() ) )
        {
            collection.add( PredefinedRoles.PUBLIC );
        }
        return collection;
    }

    class NeoShiroSubject implements AuthSubject
    {

        @Override
        public String username()
        {
            Object principal = shiroSubject.getPrincipal();
            if ( principal != null )
            {
                return principal.toString();
            }
            else
            {
                return ""; // Should never clash with a valid username
            }
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return shiroSubject.getAuthenticationResult();
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {
            if ( getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED )
            {
                shiroSubject.setAuthenticationResult( AuthenticationResult.SUCCESS );
            }
        }

        @Override
        public boolean hasUsername( String username )
        {
            Object principal = shiroSubject.getPrincipal();
            return username != null && username.equals( principal );
        }

        String getAuthenticationFailureMessage()
        {
            String message = "";
            List<Throwable> throwables = shiroSubject.getAuthenticationInfo().getThrowables();
            switch ( shiroSubject.getAuthenticationResult() )
            {
            case FAILURE:
                {
                    message = buildMessageFromThrowables( "invalid principal or credentials", throwables );
                }
                break;
            case TOO_MANY_ATTEMPTS:
                {
                    message = buildMessageFromThrowables( "too many failed attempts", throwables );
                }
                break;
            case PASSWORD_CHANGE_REQUIRED:
                {
                    message = buildMessageFromThrowables( "password change required", throwables );
                }
                break;
            default:
            }
            return message;
        }

        void clearAuthenticationInfo()
        {
            shiroSubject.clearAuthenticationInfo();
        }
    }

    private static String buildMessageFromThrowables( String baseMessage, List<Throwable> throwables )
    {
        if ( throwables == null )
        {
            return baseMessage;
        }

        StringBuilder sb = new StringBuilder( baseMessage );

        for ( Throwable t : throwables )
        {
            if ( t.getMessage() != null )
            {
                sb.append( " (" );
                sb.append( t.getMessage() );
                sb.append( ")" );
            }
            Throwable cause = t.getCause();
            if ( cause != null && cause.getMessage() != null )
            {
                sb.append( " (" );
                sb.append( cause.getMessage() );
                sb.append( ")" );
            }
            Throwable causeCause = cause != null ? cause.getCause() : null;
            if ( causeCause != null && causeCause.getMessage() != null )
            {
                sb.append( " (" );
                sb.append( causeCause.getMessage() );
                sb.append( ")" );
            }
        }
        return sb.toString();
    }
}
