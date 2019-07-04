/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import org.apache.shiro.authz.AuthorizationInfo;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.LabelSet;
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

        Set<ResourcePrivilege> privileges = authManager.getPermissions( roles );
        for ( ResourcePrivilege privilege : privileges )
        {
            String privilegeDbName = privilege.getDbName();
            if ( privilege.isAllDatabases() || privilegeDbName.equals( dbName ) )
            {
                accessModeBuilder.addPrivilege( privilege );
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
        private final boolean allowsTokenCreates;
        private final boolean allowsSchemaWrites;
        private final boolean isAdmin;
        private final boolean passwordChangeRequired;
        private final Set<String> roles;
        private final IntPredicate propertyPermissions;

        private final boolean allowsTraverseAllLabels;
        private final boolean allowsTraverseAllRelTypes;
        private final IntSet whitelistTraverseLabels;
        private final IntSet whitelistTraverseRelTypes;

        private final boolean disallowsTraverseAllLabels;
        private final boolean disallowsTraverseAllRelTypes;
        private final IntSet blacklistTraverseLabels;
        private final IntSet blacklistTraverseRelTypes;

        private final boolean allowsReadAllPropertiesAllLabels;
        private final boolean allowsReadAllPropertiesAllRelTypes;
        private final IntSet whitelistedLabelsForAllProperties;
        private final IntSet whitelistedRelTypesForAllProperties;
        private final IntSet whitelistedNodeProperties;
        private final IntSet whitelistedRelationshipProperties;
        private final IntObjectMap<IntSet> whitelistedLabelsForProperty;
        private final IntObjectMap<IntSet> whitelistedRelTypesForProperty;

        StandardAccessMode(
                boolean allowsReads,
                boolean allowsWrites,
                boolean allowsTokenCreates,
                boolean allowsSchemaWrites,
                boolean isAdmin,
                boolean passwordChangeRequired,
                Set<String> roles,
                IntPredicate propertyPermissions,

                boolean allowsTraverseAllLabels,
                boolean allowsTraverseAllRelTypes,
                IntSet whitelistTraverseLabels,
                IntSet whitelistTraverseRelTypes,

                boolean disallowsTraverseAllLabels,
                boolean disallowsTraverseAllRelTypes,
                IntSet blacklistTraverseLabels,
                IntSet blacklistTraverseRelTypes,

                boolean allowsReadAllPropertiesAllLabels,
                boolean allowsReadAllPropertiesAllRelTypes,
                IntSet whitelistedLabelsForAllProperties,
                IntSet whitelistedRelTypesForAllProperties,
                IntSet whitelistedNodeProperties,
                IntSet whitelistedRelationshipProperties,
                IntObjectMap<IntSet> whitelistedLabelsForProperty,
                IntObjectMap<IntSet> whitelistedRelTypesForProperty
        )
        {
            this.allowsReads = allowsReads;
            this.allowsWrites = allowsWrites;
            this.allowsTokenCreates = allowsTokenCreates;
            this.allowsSchemaWrites = allowsSchemaWrites;
            this.isAdmin = isAdmin;
            this.passwordChangeRequired = passwordChangeRequired;
            this.roles = roles;
            this.propertyPermissions = propertyPermissions;

            this.allowsTraverseAllLabels = allowsTraverseAllLabels;
            this.allowsTraverseAllRelTypes = allowsTraverseAllRelTypes;
            this.whitelistTraverseLabels = whitelistTraverseLabels;
            this.whitelistTraverseRelTypes = whitelistTraverseRelTypes;

            this.disallowsTraverseAllLabels = disallowsTraverseAllLabels;
            this.disallowsTraverseAllRelTypes = disallowsTraverseAllRelTypes;
            this.blacklistTraverseLabels = blacklistTraverseLabels;
            this.blacklistTraverseRelTypes = blacklistTraverseRelTypes;

            this.allowsReadAllPropertiesAllLabels = allowsReadAllPropertiesAllLabels;
            this.allowsReadAllPropertiesAllRelTypes = allowsReadAllPropertiesAllRelTypes;
            this.whitelistedLabelsForAllProperties = whitelistedLabelsForAllProperties;
            this.whitelistedRelTypesForAllProperties = whitelistedRelTypesForAllProperties;
            this.whitelistedNodeProperties = whitelistedNodeProperties;
            this.whitelistedRelationshipProperties = whitelistedRelationshipProperties;
            this.whitelistedLabelsForProperty = whitelistedLabelsForProperty;
            this.whitelistedRelTypesForProperty = whitelistedRelTypesForProperty;
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
            return allowsTraverseAllLabels && !disallowsTraverseAllLabels && blacklistTraverseLabels.isEmpty();
        }

        @Override
        public boolean allowsTraverseLabels( long... labels )
        {
            if ( allowsTraverseAllLabels() )
            {
                return true;
            }
            if ( disallowsTraverseAllLabels )
            {
                return false;
            }

            boolean allowedTraverseAnyLabel = false;

            for ( long labelAsLong : labels )
            {
                int label = (int) labelAsLong;
                if ( blacklistTraverseLabels.contains( label ) )
                {
                    return false;
                }
                else if ( whitelistTraverseLabels.contains( label ) )
                {
                    allowedTraverseAnyLabel = true;
                }
            }

            return allowedTraverseAnyLabel || allowsTraverseAllLabels;
        }

        @Override
        public boolean allowsTraverseAllRelTypes()
        {
            return allowsTraverseAllRelTypes && !disallowsTraverseAllRelTypes && blacklistTraverseRelTypes.isEmpty();
        }

        @Override
        public boolean allowsTraverseRelType( int relType )
        {
            if ( allowsTraverseAllRelTypes() )
            {
                return true;
            }
            if ( disallowsTraverseAllRelTypes || blacklistTraverseRelTypes.contains( relType ) )
            {
                return false;
            }

            return whitelistTraverseRelTypes.contains( relType ) || allowsTraverseAllRelTypes;
        }

        @Override
        public boolean allowsReadPropertyAllLabels( int propertyKey )
        {
            return allowsReadAllPropertiesAllLabels || whitelistedNodeProperties.contains( propertyKey );
        }

        @Override
        public boolean allowsReadNodeProperty( Supplier<LabelSet> labelSupplier, int propertyKey )
        {
            if ( allowsReadPropertyAllLabels( propertyKey ) )
            {
                return true;
            }

            LabelSet labelSet = labelSupplier.get();
            IntSet whiteListed = whitelistedLabelsForProperty.get( propertyKey );
            for ( long labelAsLong : labelSet.all() )
            {
                int label = (int) labelAsLong;
                if ( whiteListed != null && whiteListed.contains( label ) )
                {
                    return true;
                }
                if ( whitelistedLabelsForAllProperties.contains( label ) )
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean allowsReadPropertyAllRelTypes( int propertyKey )
        {
            return allowsReadAllPropertiesAllRelTypes || whitelistedRelationshipProperties.contains( propertyKey );
        }

        @Override
        public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
        {
            if ( allowsReadPropertyAllRelTypes( propertyKey ) )
            {
                return true;
            }
            IntSet whitelisted = whitelistedRelTypesForProperty.get( propertyKey );
            return whitelisted != null && whitelisted.contains( relType.getAsInt() ) || whitelistedRelTypesForAllProperties.contains( relType.getAsInt() );
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
            private boolean allowsWrite;
            private boolean disallowsWrite;
            private boolean token;
            private boolean schema;
            private boolean admin;
            private IntPredicate propertyPermissions;

            private boolean allowsTraverseAllLabels;
            private boolean allowsTraverseAllRelTypes;
            private MutableIntSet whitelistTraverseLabels = IntSets.mutable.empty();
            private MutableIntSet whitelistTraverseRelTypes = IntSets.mutable.empty();

            private boolean disallowsTraverseAllLabels;
            private boolean disallowsTraverseAllRelTypes;
            private MutableIntSet blacklistTraverseLabels = IntSets.mutable.empty();
            private MutableIntSet blackListTraverseRelTypes = IntSets.mutable.empty();

            private boolean allowReadAllPropertiesAllLabels;
            private boolean allowReadAllPropertiesAllRelTypes;
            private MutableIntSet allowedNodeSegmentForAllProperties = IntSets.mutable.empty();
            private MutableIntSet allowedRelationshipSegmentForAllProperties = IntSets.mutable.empty();
            private MutableIntSet whitelistNodeProperties = IntSets.mutable.empty();
            private MutableIntSet whitelistRelationshipProperties = IntSets.mutable.empty();
            private MutableIntObjectMap<IntSet> allowedNodeSegmentForProperty = IntObjectMaps.mutable.empty();
            private MutableIntObjectMap<IntSet> allowedRelationshipSegmentForProperty = IntObjectMaps.mutable.empty();

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
                        isAuthenticated && allowsWrite && !disallowsWrite,
                        isAuthenticated && token,
                        isAuthenticated && schema,
                        isAuthenticated && admin,
                        passwordChangeRequired,
                        roles,
                        propertyPermissions,
                        allowsTraverseAllLabels,
                        allowsTraverseAllRelTypes,
                        whitelistTraverseLabels,
                        whitelistTraverseRelTypes,
                        disallowsTraverseAllLabels,
                        disallowsTraverseAllRelTypes,
                        blacklistTraverseLabels,
                        blackListTraverseRelTypes,
                        allowReadAllPropertiesAllLabels,
                        allowReadAllPropertiesAllRelTypes,
                        allowedNodeSegmentForAllProperties,
                        allowedRelationshipSegmentForAllProperties,
                        whitelistNodeProperties,
                        whitelistRelationshipProperties,
                        allowedNodeSegmentForProperty,
                        allowedRelationshipSegmentForProperty );
            }

            void addPrivilege( ResourcePrivilege privilege ) throws KernelException
            {
                Resource resource = privilege.getResource();
                Segment segment = privilege.getSegment();
                boolean allowed = privilege.isAllowed();

                switch ( privilege.getAction() )
                {
                case FIND:
                    read = true;
                    if ( segment instanceof LabelSegment )
                    {
                        if ( segment.equals( LabelSegment.ALL ) )
                        {
                            if ( allowed )
                            {
                                allowsTraverseAllLabels = true;
                            }
                            else
                            {
                                disallowsTraverseAllLabels = true;
                            }
                        }
                        else
                        {
                            int labelId = resolveLabelId( ((LabelSegment) segment).getLabel() );
                            if ( allowed )
                            {
                                whitelistTraverseLabels.add( labelId );
                            }
                            else
                            {
                                blacklistTraverseLabels.add( labelId );
                            }
                        }
                    }
                    else if ( segment instanceof RelTypeSegment )
                    {
                        if ( segment.equals( RelTypeSegment.ALL ) )
                        {
                            if ( allowed )
                            {
                                allowsTraverseAllRelTypes = true;
                            }
                            else
                            {
                                disallowsTraverseAllRelTypes = true;
                            }
                        }
                        else
                        {
                            int relTypeId = resolveRelTypeId( ((RelTypeSegment) segment).getRelType() );
                            if ( allowed )
                            {
                                whitelistTraverseRelTypes.add( relTypeId );
                            }
                            else
                            {
                                blackListTraverseRelTypes.add( relTypeId );
                            }
                        }
                    }
                    else
                    {
                        throw new IllegalStateException( "Unsupported segment qualifier for find privilege: " + segment.getClass().getSimpleName() );
                    }

                    break;
                case READ:
                    switch ( resource.type() )
                    {
                    case GRAPH:
                        allowReadAllPropertiesAllLabels = true;
                        allowReadAllPropertiesAllRelTypes = true;
                        read = true;
                        break;
                    case PROPERTY:
                        read = true;
                        int propertyId = resolvePropertyId( resource.getArg1() );
                        if ( segment instanceof LabelSegment )
                        {
                            if ( segment.equals( LabelSegment.ALL ) )
                            {
                                whitelistNodeProperties.add( propertyId );
                            }
                            else
                            {
                                MutableIntSet allowedNodesWithLabels = (MutableIntSet) allowedNodeSegmentForProperty
                                        .getIfAbsentPut( propertyId, IntSets.mutable.empty() );
                                int labelId = resolveLabelId( ((LabelSegment) segment).getLabel() );
                                allowedNodesWithLabels.add( labelId );
                            }
                        }
                        else if ( segment instanceof RelTypeSegment )
                        {
                            if ( segment.equals( RelTypeSegment.ALL ) )
                            {
                                whitelistRelationshipProperties.add( propertyId );
                            }
                            else
                            {
                                MutableIntSet allowedWithRelType = (MutableIntSet) allowedRelationshipSegmentForProperty
                                        .getIfAbsentPut( propertyId, IntSets.mutable.empty() );
                                int relTypeId = resolveRelTypeId( ((RelTypeSegment) segment).getRelType() );
                                allowedWithRelType.add( relTypeId );
                            }
                        }
                        else
                        {
                            throw new IllegalStateException( "Unsupported segment qualifier for read privilege: " + segment.getClass().getSimpleName() );
                        }
                        break;
                    case ALL_PROPERTIES:
                        read = true;
                        if ( segment instanceof LabelSegment )
                        {
                            if ( segment.equals( LabelSegment.ALL ) )
                            {
                                allowReadAllPropertiesAllLabels = true;
                            }
                            else
                            {
                                int labelId = resolveLabelId( ((LabelSegment) segment).getLabel() );
                                allowedNodeSegmentForAllProperties.add( labelId );
                            }
                        }
                        else if ( segment instanceof RelTypeSegment )
                        {
                            if ( segment.equals( RelTypeSegment.ALL ) )
                            {
                                allowReadAllPropertiesAllRelTypes = true;
                            }
                            else
                            {
                                int relTypeId = resolveRelTypeId( ((RelTypeSegment) segment).getRelType() );
                                allowedRelationshipSegmentForAllProperties.add( relTypeId );
                            }
                        }
                        else
                        {
                            throw new IllegalStateException( "Unsupported segment qualifier for read privilege: " + segment.getClass().getSimpleName() );
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
                    case ALL_PROPERTIES:
                        if ( allowed )
                        {
                            allowsWrite = true;
                        }
                        else
                        {
                            disallowsWrite = true;
                        }
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

            private int resolveLabelId( String label ) throws KernelException
            {
                assert !label.isEmpty();
                return resolver.getOrCreateLabelId( label );
            }

            private int resolveRelTypeId( String relType ) throws KernelException
            {
                assert !relType.isEmpty();
                return resolver.getOrCreateRelTypeId( relType );
            }

            private int resolvePropertyId( String property ) throws KernelException
            {
                assert !property.isEmpty();
                return resolver.getOrCreatePropertyKeyId( property );
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
