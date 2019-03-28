/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import org.apache.shiro.authz.AuthorizationInfo;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

public class StandardCommercialLoginContext implements CommercialLoginContext
{
    private final MultiRealmAuthManager authManager;
    private final ShiroSubject shiroSubject;
    private final NeoShiroSubject neoShiroSubject;

    StandardCommercialLoginContext( MultiRealmAuthManager authManager, ShiroSubject shiroSubject )
    {
        this.authManager = authManager;
        this.shiroSubject = shiroSubject;
        this.neoShiroSubject = new NeoShiroSubject();
    }

    @Override
    public AuthSubject subject()
    {
        return neoShiroSubject;
    }

    private StandardAccessMode mode( IdLookup resolver, String dbName )
    {
        boolean isAuthenticated = shiroSubject.isAuthenticated();
        boolean passwordChangeRequired = shiroSubject.getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        Set<String> roles = queryForRoleNames();
        StandardAccessMode.Builder accessModeBuilder = new StandardAccessMode.Builder( isAuthenticated, passwordChangeRequired, roles, resolver );

        Set<DatabasePrivilege> privileges = authManager.getPermissions( roles );
        for ( DatabasePrivilege privilege : privileges )
        {
            String privilegeDbName = privilege.getDbName();
            if ( privilegeDbName.equals( dbName ) || privilegeDbName.equals( "*" ) )
            {
                accessModeBuilder.addPrivileges( privilege );
            }
        }

        accessModeBuilder.addPropertyPermissions( queryForPropertyPermissions( resolver ) );
        return accessModeBuilder.build();
    }

    @Override
    public CommercialSecurityContext authorize( IdLookup idLookup, String dbName )
    {
        StandardAccessMode mode = mode( idLookup, dbName );
        return new CommercialSecurityContext( neoShiroSubject, mode, mode.roles, mode.isAdmin() );
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
        return authorizationInfo.stream()
                .flatMap( authInfo ->
                {
                    Collection<String> roles = authInfo.getRoles();
                    return roles == null ? Stream.empty() : roles.stream();
                } )
                .collect( Collectors.toSet() );
    }

    private IntPredicate queryForPropertyPermissions( IdLookup resolver )
    {
        return authManager.getPropertyPermissions( roles(), resolver );
    }

    private static class StandardAccessMode implements AccessMode
    {
        private final boolean allowsReads;
        private final boolean allowsReadAllLabels;
        private final boolean allowsWrites;
        private final boolean allowsSchemaWrites;
        private final boolean allowsTokenCreates;
        private final boolean passwordChangeRequired;
        private final Set<String> roles;
        private final Set<Integer> whitelistedLabels;
        private final IntPredicate propertyPermissions;
        private final boolean isAdmin;

        StandardAccessMode( boolean allowsReads, boolean allowsWrites, boolean allowsTokenCreates, boolean allowsSchemaWrites,
                boolean isAdmin, boolean passwordChangeRequired, Set<String> roles, IntPredicate propertyPermissions, boolean allowsReadAllLabels,
                Set<Integer> whitelistedLabels )
        {
            this.allowsReads = allowsReads;
            this.allowsWrites = allowsWrites;
            this.allowsTokenCreates = allowsTokenCreates;
            this.allowsSchemaWrites = allowsSchemaWrites;
            this.isAdmin = isAdmin;
            this.passwordChangeRequired = passwordChangeRequired;
            this.roles = roles;
            this.propertyPermissions = propertyPermissions;
            this.whitelistedLabels = whitelistedLabels;
            this.allowsReadAllLabels = allowsReadAllLabels;
        }

        @Override
        public boolean allowsReads()
        {
            return allowsReads;
        }

        @Override
        public boolean allowsWrites()
        {
            return allowsWrites;
        }

        @Override
        public boolean allowsTokenCreates()
        {
            return allowsTokenCreates;
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return allowsSchemaWrites;
        }

        @Override
        public boolean allowsReadAllLabels()
        {
            return allowsReadAllLabels;
        }

        @Override
        public boolean allowsReadLabels( IntStream labels )
        {
            if ( allowsReadAllLabels )
            {
                return true;
            }
            // it is enough to have allows on one label to read the node
            return labels.anyMatch( whitelistedLabels::contains );
        }

        @Override
        public boolean allowsPropertyReads( int propertyKey )
        {
            return propertyPermissions.test( propertyKey );
        }

        @Override
        public boolean allowsProcedureWith( String[] roleNames )
        {
            for ( String roleName : roleNames )
            {
                if ( roles.contains( roleName ) )
                {
                    return true;
                }
            }
            return false;
        }

        public boolean isAdmin()
        {
            return isAdmin;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            if ( passwordChangeRequired )
            {
                return AccessMode.Static.CREDENTIALS_EXPIRED.onViolation( msg );
            }
            else
            {
                return new AuthorizationViolationException( msg );
            }
        }

        @Override
        public String name()
        {
            Set<String> sortedRoles = new TreeSet<>( roles );
            return roles.isEmpty() ? "no roles" : "roles [" + String.join( ",", sortedRoles ) + "]";
        }

        static class Builder
        {
            private final boolean isAuthenticated;
            private final boolean passwordChangeRequired;
            private final Set<String> roles;
            private final IdLookup resolver;

            private boolean read;
            private boolean write;
            private boolean token;
            private boolean schema;
            private boolean admin;
            private IntPredicate propertyPermissions;
            private boolean allowReadAllNodes;
            private Set<Integer> allowedLabels;

            Builder( boolean isAuthenticated, boolean passwordChangeRequired, Set<String> roles, IdLookup resolver )
            {
                this.isAuthenticated = isAuthenticated;
                this.passwordChangeRequired = passwordChangeRequired;
                this.roles = roles;
                this.resolver = resolver;
                allowedLabels = new HashSet<>();
            }

            void addPropertyPermissions( IntPredicate propertyPermissions )
            {

                this.propertyPermissions = propertyPermissions;
            }

            StandardAccessMode build()
            {
                return new StandardAccessMode(
                        isAuthenticated && read,
                        isAuthenticated && write,
                        isAuthenticated && token,
                        isAuthenticated && schema,
                        isAuthenticated && admin,
                        passwordChangeRequired,
                        roles,
                        propertyPermissions,
                        allowReadAllNodes,
                        allowedLabels );
            }

            void addPrivileges( DatabasePrivilege dbPrivilege )
            {
                for ( ResourcePrivilege privilege : dbPrivilege.getPrivileges() )
                {
                    Resource resource = privilege.getResource();
                    switch ( privilege.getAction() )
                    {
                    case READ:
                        switch ( resource.type() )
                        {
                        case TOKEN:
                        case SCHEMA:
                        case SYSTEM:
                        case PROCEDURE:
                            break;
                        case GRAPH:
                            read = true;
                            if ( resource instanceof Resource.LabelResource )
                            {
                                String label = resource.getArg1();
                                if ( label.isEmpty() )
                                {
                                    allowReadAllNodes = true;
                                }
                                else
                                {
                                    try
                                    {
                                        allowedLabels.add( resolver.getOrCreateLabelId( label ) );
                                    }
                                    catch ( KernelException ignored )
                                    {
                                    }
                                }
                            }
                            else
                            {
                                allowReadAllNodes = true;
                            }
                            break;
                        default:
                        }
                        break;
                    case WRITE:
                        switch ( resource.type() )
                        {
                        case GRAPH:
                            write = true;
                            break;
                        case TOKEN:
                            token = true;
                            break;
                        case SCHEMA:
                            schema = true;
                            break;
                        case SYSTEM:
                            admin = true;
                            break;
                        case PROCEDURE:
                            break;
                        default:
                        }
                        break;
                    case EXECUTE:
                        switch ( resource.type() )
                        {
                        case GRAPH:
                        case TOKEN:
                        case SCHEMA:
                        case SYSTEM:
                            break;
                        case PROCEDURE:
                            // implement when porting procedure execute privileges to system graph
                            break;
                        default:
                        }
                        break;
                    default:
                    }
                }
            }
        }
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
        public void logout()
        {
            shiroSubject.logout();
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
