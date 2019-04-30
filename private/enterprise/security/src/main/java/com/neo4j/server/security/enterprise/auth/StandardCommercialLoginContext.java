/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import org.apache.shiro.authz.AuthorizationInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
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

    private StandardAccessMode mode( IdLookup resolver, String dbName ) throws KernelException
    {
        boolean isAuthenticated = shiroSubject.isAuthenticated();
        boolean passwordChangeRequired = shiroSubject.getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        Set<String> roles = queryForRoleNames();
        StandardAccessMode.Builder accessModeBuilder = new StandardAccessMode.Builder( isAuthenticated, passwordChangeRequired, roles, resolver );

        Set<DatabasePrivilege> privileges = authManager.getPermissions( roles );
        for ( DatabasePrivilege privilege : privileges )
        {
            String privilegeDbName = privilege.getDbName();
            if ( privilege.isAllDatabases() || privilegeDbName.equals( dbName ) )
            {
                accessModeBuilder.addPrivileges( privilege );
            }
        }

        accessModeBuilder.addPropertyPermissions( queryForPropertyPermissions( resolver ) );
        return accessModeBuilder.build();
    }

    @Override
    public CommercialSecurityContext authorize( IdLookup idLookup, String dbName ) throws KernelException
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

    private IntPredicate queryForPropertyPermissions( IdLookup resolver ) throws KernelException
    {
        return authManager.getPropertyPermissions( roles(), resolver );
    }

    private static class StandardAccessMode implements AccessMode
    {
        private final boolean allowsReads;
        private final boolean allowsWrites;
        private final boolean allowsSchemaWrites;
        private final boolean allowsTokenCreates;
        private final boolean passwordChangeRequired;
        private final Set<String> roles;
        private final boolean allowsReadAllPropertiesAllLabels;
        private final Set<Integer> whitelistedNodeProperties;
        private final Map<Integer, Set<Integer>> whitelistedLabelsForProperty;
        private final IntPredicate propertyPermissions; // TODO translate this to blacklistedPropertiesForAllLabels
        private final boolean isAdmin;
        private final boolean allowsTraverseAllLabels;
        private final Set<Integer> whitelistTraverseLabels;

        StandardAccessMode( boolean allowsReads, boolean allowsWrites, boolean allowsTokenCreates, boolean allowsSchemaWrites,
                boolean isAdmin, boolean passwordChangeRequired, Set<String> roles, IntPredicate propertyPermissions,
                boolean allowsReadAllPropertiesAllLabels, Map<Integer,Set<Integer>> whitelistedLabelsForProperty, Set<Integer> whitelistedNodeProperties,
                boolean allowsTraverseAllLabels, Set<Integer> whitelistTraverseLabels )
        {
            this.allowsReads = allowsReads;
            this.allowsWrites = allowsWrites;
            this.allowsTokenCreates = allowsTokenCreates;
            this.allowsSchemaWrites = allowsSchemaWrites;
            this.isAdmin = isAdmin;
            this.passwordChangeRequired = passwordChangeRequired;
            this.roles = roles;
            this.propertyPermissions = propertyPermissions;
            this.allowsReadAllPropertiesAllLabels = allowsReadAllPropertiesAllLabels;
            this.whitelistedLabelsForProperty = whitelistedLabelsForProperty;
            this.whitelistedNodeProperties = whitelistedNodeProperties;
            this.allowsTraverseAllLabels = allowsTraverseAllLabels;
            this.whitelistTraverseLabels = whitelistTraverseLabels;
        }

        @Override
        public boolean allowsTokenReads()
        {
            return allowsReads || allowsWrites || allowsTokenCreates || allowsSchemaWrites;
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
        public boolean allowsTraverseAllLabels()
        {
            return allowsTraverseAllLabels;
        }

        @Override
        public boolean allowsTraverseLabels( IntStream labels )
        {
            return allowsTraverseAllLabels || labels.anyMatch( whitelistTraverseLabels::contains );
        }

        @Override
        public boolean allowsReadPropertyAllLabels( int propertyKey )
        {
            return allowsReadAllPropertiesAllLabels || whitelistedNodeProperties.contains( propertyKey );
        }

        @Override
        public boolean allowsReadProperty( Supplier<int[]> labelSupplier, int propertyKey )
        {
            IntStream labels = Arrays.stream( labelSupplier.get() );
            return allowsReadPropertyAllLabels( propertyKey ) ||
                    labels.anyMatch( l1 -> whitelistedLabelsForProperty.getOrDefault( propertyKey, Collections.emptySet() ).contains( l1 ) ) ||
                    Arrays.stream( labelSupplier.get() ).anyMatch( l -> whitelistedLabelsForProperty.getOrDefault( -1, Collections.emptySet() ).contains( l ) );
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
            private boolean whitelistAllPropertiesInWholeGraph;
            private Map<Integer, Set<Integer>> allowedSegmentForProperty = new HashMap<>();
            private Set<Integer> whitelistNodeProperties = new HashSet<>();
            private boolean allowsTraverseAllLabels;
            private Set<Integer> whitelistTraverseLabels = new HashSet<>();

            Builder( boolean isAuthenticated, boolean passwordChangeRequired, Set<String> roles, IdLookup resolver )
            {
                this.isAuthenticated = isAuthenticated;
                this.passwordChangeRequired = passwordChangeRequired;
                this.roles = roles;
                this.resolver = resolver;
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
                        whitelistAllPropertiesInWholeGraph,
                        allowedSegmentForProperty,
                        whitelistNodeProperties,
                        allowsTraverseAllLabels,
                        whitelistTraverseLabels );
            }

            void addPrivileges( DatabasePrivilege dbPrivilege ) throws KernelException
            {
                for ( ResourcePrivilege privilege : dbPrivilege.getPrivileges() )
                {
                    Resource resource = privilege.getResource();
                    switch ( privilege.getAction() )
                    {
                    case FIND:
                        read = true;
                        if ( privilege.getSegment().equals( Segment.ALL ) )
                        {
                            allowsTraverseAllLabels = true;
                        }
                        else
                        {
                            for ( String label : privilege.getSegment().getLabels() )
                            {
                                int labelId = resolveLabelId( label );
                                whitelistTraverseLabels.add( labelId );
                            }
                        }
                        break;
                    case READ:
                        switch ( resource.type() )
                        {
                        case TOKEN:
                        case SCHEMA:
                        case SYSTEM:
                        case PROCEDURE:
                            break;
                        case GRAPH:
                        case PROPERTY:
                        case ALL_PROPERTIES:
                            read = true;
                            if ( resource instanceof Resource.PropertyResource )
                            {
                                int propertyId = resolvePropertyId( resource.getArg1() );

                                if ( privilege.getSegment() == Segment.ALL )
                                {
                                    if ( propertyId == -1 ) // TODO magic number
                                    {
                                        whitelistAllPropertiesInWholeGraph = true;
                                    }
                                    else
                                    {
                                        whitelistNodeProperties.add( propertyId );
                                    }
                                }
                                else
                                {
                                    Set<Integer> allowedNodesWithLabels = allowedSegmentForProperty.computeIfAbsent( propertyId, label -> new HashSet<>() );
                                    for ( String label : privilege.getSegment().getLabels() )
                                    {
                                        int labelId = resolveLabelId( label );
                                        allowedNodesWithLabels.add( labelId );
                                    }
                                }
                            }
                            else if ( resource instanceof Resource.AllPropertiesResource )
                            {

                            }
                            else
                            {
                                whitelistAllPropertiesInWholeGraph = true;
                            }
                            break;
                        default:
                        }
                        break;
                    case WRITE:
                        switch ( resource.type() )
                        {
                        case GRAPH:
                        case PROPERTY:
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

            private int resolveLabelId( String label ) throws KernelException
            {
                return resolver.getOrCreateLabelId( label );
            }

            private int resolvePropertyId( String property ) throws KernelException
            {
                if ( !property.isEmpty() )
                {
                    return resolver.getOrCreatePropertyKeyId( property );
                }
                return -1;
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
