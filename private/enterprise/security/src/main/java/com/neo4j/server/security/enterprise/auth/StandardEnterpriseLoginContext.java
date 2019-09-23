/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.AdminAccessMode;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.apache.shiro.authz.AuthorizationInfo;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SCHEMA;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;

public class StandardEnterpriseLoginContext implements EnterpriseLoginContext
{
    private final MultiRealmAuthManager authManager;
    private final ShiroSubject shiroSubject;
    private final NeoShiroSubject neoShiroSubject;

    StandardEnterpriseLoginContext( MultiRealmAuthManager authManager, ShiroSubject shiroSubject )
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
        StandardAccessMode.Builder accessModeBuilder = new StandardAccessMode.Builder( isAuthenticated, passwordChangeRequired, roles, resolver, dbName );

        Set<ResourcePrivilege> privileges = authManager.getPermissions( roles );
        for ( ResourcePrivilege privilege : privileges )
        {
            if ( privilege.appliesTo( dbName ) )
            {
                accessModeBuilder.addPrivilege( privilege );
            }
        }
        if ( dbName.equals( SYSTEM_DATABASE_NAME ) )
        {
            accessModeBuilder.withAccess();
        }
        accessModeBuilder.addBlacklistedPropertyPermissions( authManager.getPropertyPermissions( roles(), resolver ) );

        StandardAccessMode mode = accessModeBuilder.build();
        if ( !mode.allowsAccess )
        {
            throw mode.onViolation(
                    String.format( "Database access is not allowed for user '%s' with roles %s.", neoShiroSubject.username(), roles.toString() ) );
        }

        return mode;
    }

    @Override
    public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName ) throws KernelException
    {
        if ( !shiroSubject.isAuthenticated() )
        {
            throw new AuthorizationViolationException( AuthorizationViolationException.PERMISSION_DENIED, Status.Security.Unauthorized );
        }
        StandardAccessMode mode = mode( idLookup, dbName );
        return new EnterpriseSecurityContext( neoShiroSubject, mode, mode.roles, mode.adminAccessMode );
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

    private static class StandardAccessMode implements AccessMode
    {
        private final boolean allowsAccess;
        private final boolean allowsReads;
        private final boolean allowsWrites;
        private final boolean allowsTokenCreates;
        private final boolean allowsSchemaWrites;
        private final boolean passwordChangeRequired;
        private final Set<String> roles;

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

        private final boolean disallowsReadAllPropertiesAllLabels;
        private final boolean disallowsReadAllPropertiesAllRelTypes;
        private final IntSet blacklistedNodeProperties;
        private final IntSet blacklistedRelationshipProperties;
        private final IntSet blacklistedLabelsForAllProperties;
        private final IntSet blacklistedRelTypesForAllProperties;
        private final IntObjectMap<IntSet> blacklistedLabelsForProperty;
        private final IntObjectMap<IntSet> blacklistedRelTypesForProperty;

        private AdminAccessMode adminAccessMode;
        private AdminActionOnResource.DatabaseScope database;

        StandardAccessMode(
                boolean allowsAccess,
                boolean allowsReads,
                boolean allowsWrites,
                boolean allowsTokenCreates,
                boolean allowsSchemaWrites,
                boolean passwordChangeRequired,
                Set<String> roles,

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
                IntObjectMap<IntSet> whitelistedRelTypesForProperty,

                boolean disallowsReadAllPropertiesAllLabels,
                boolean disallowsReadAllPropertiesAllRelTypes,
                IntSet blacklistedLabelsForAllProperties,
                IntSet blacklistedRelTypesForAllProperties,
                IntSet blacklistedNodeProperties,
                IntSet blacklistedRelationshipProperties,
                IntObjectMap<IntSet> blacklistedLabelsForProperty,
                IntObjectMap<IntSet> blacklistedRelTypesForProperty,

                AdminAccessMode adminAccessMode,
                String database
        )
        {
            this.allowsAccess = allowsAccess;
            this.allowsReads = allowsReads;
            this.allowsWrites = allowsWrites;
            this.allowsTokenCreates = allowsTokenCreates;
            this.allowsSchemaWrites = allowsSchemaWrites;
            this.passwordChangeRequired = passwordChangeRequired;
            this.roles = roles;

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

            this.disallowsReadAllPropertiesAllLabels = disallowsReadAllPropertiesAllLabels;
            this.disallowsReadAllPropertiesAllRelTypes = disallowsReadAllPropertiesAllRelTypes;
            this.blacklistedLabelsForAllProperties = blacklistedLabelsForAllProperties;
            this.blacklistedRelTypesForAllProperties = blacklistedRelTypesForAllProperties;
            this.blacklistedNodeProperties = blacklistedNodeProperties;
            this.blacklistedRelationshipProperties = blacklistedRelationshipProperties;
            this.blacklistedLabelsForProperty = blacklistedLabelsForProperty;
            this.blacklistedRelTypesForProperty = blacklistedRelTypesForProperty;

            this.adminAccessMode = adminAccessMode;
            this.database = new AdminActionOnResource.DatabaseScope( database );
        }

        @Override
        public boolean allowsWrites()
        {
            return allowsWrites;
        }

        @Override
        public boolean allowsTokenCreates( PrivilegeAction action )
        {
            return allowsTokenCreates && adminAccessMode.allows( new AdminActionOnResource( action, database ) );
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return allowsSchemaWrites;
        }

        @Override
        public boolean allowsSchemaWrites( PrivilegeAction action )
        {
            return allowsSchemaWrites && adminAccessMode.allows( new AdminActionOnResource( action, database ) );
        }

        @Override
        public boolean allowsTraverseAllLabels()
        {
            return allowsTraverseAllLabels && !disallowsTraverseAllLabels && blacklistTraverseLabels.isEmpty();
        }

        @Override
        public boolean allowsTraverseLabel( long label )
        {
            // Note: we do not check blacklistTraverseLabels.contains(label) because this should be a first check
            // to be followed by the explicit blacklist check in disallowsTraverseLabel
            if ( disallowsTraverseAllLabels || blacklistTraverseLabels.notEmpty() )
            {
                return false;
            }
            return allowsTraverseAllLabels || whitelistTraverseLabels.contains( (int) label );
        }

        @Override
        public boolean disallowsTraverseLabel( long label )
        {
            return disallowsTraverseAllLabels || blacklistTraverseLabels.contains( (int) label );
        }

        @Override
        public boolean disallowsTraverseType( long type )
        {
            return disallowsTraverseAllRelTypes || blacklistTraverseRelTypes.contains( (int) type );
        }

        @Override
        public boolean allowsTraverseNodeLabels( long... labels )
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

            return allowsTraverseAllRelTypes || whitelistTraverseRelTypes.contains( relType );
        }

        @Override
        public boolean allowsReadPropertyAllLabels( int propertyKey )
        {
            return (allowsReadAllPropertiesAllLabels || whitelistedNodeProperties.contains( propertyKey )) && !disallowsReadPropertyForSomeLabel( propertyKey );
        }

        @Override
        public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
        {
            return disallowsReadAllPropertiesAllLabels || blacklistedNodeProperties.contains( propertyKey ) || !blacklistedLabelsForAllProperties.isEmpty() ||
                    blacklistedLabelsForProperty.get( propertyKey ) != null;
        }

        private boolean disallowsReadPropertyForAllLabels( int propertyKey )
        {
            return disallowsReadAllPropertiesAllLabels || blacklistedNodeProperties.contains( propertyKey );
        }

        @Override
        public boolean allowsReadNodeProperty( Supplier<LabelSet> labelSupplier, int propertyKey )
        {
            if ( allowsReadPropertyAllLabels( propertyKey ) )
            {
                return true;
            }

            if ( disallowsReadPropertyForAllLabels( propertyKey ) )
            {
                return false;
            }

            LabelSet labelSet = labelSupplier.get();
            IntSet whiteListed = whitelistedLabelsForProperty.get( propertyKey );
            IntSet blackListed = blacklistedLabelsForProperty.get( propertyKey );

            boolean allowed = false;

            for ( long labelAsLong : labelSet.all() )
            {
                int label = (int) labelAsLong;
                if ( whiteListed != null && whiteListed.contains( label ) )
                {
                    allowed = true;
                }
                if ( blackListed != null && blackListed.contains( label ) )
                {
                    return false;
                }
                if ( whitelistedLabelsForAllProperties.contains( label ) )
                {
                    allowed = true;
                }
                if ( blacklistedLabelsForAllProperties.contains( label ) )
                {
                    return false;
                }
            }
            return allowed || allowsReadAllPropertiesAllLabels || whitelistedNodeProperties.contains( propertyKey );
        }

        @Override
        public boolean allowsReadPropertyAllRelTypes( int propertyKey )
        {
            return (allowsReadAllPropertiesAllRelTypes || whitelistedRelationshipProperties.contains( propertyKey )) &&
                    !disallowsReadPropertyForSomeRelType( propertyKey );
        }

        private boolean disallowsReadPropertyForSomeRelType( int propertyKey )
        {
            return disallowsReadAllPropertiesAllRelTypes || blacklistedRelationshipProperties.contains( propertyKey ) ||
                    !blacklistedRelTypesForAllProperties.isEmpty() || blacklistedRelTypesForProperty.get( propertyKey ) != null;
        }

        private boolean disallowsReadPropertyForAllRelTypes( int propertyKey )
        {
            return disallowsReadAllPropertiesAllRelTypes || blacklistedRelationshipProperties.contains( propertyKey );
        }

        @Override
        public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
        {
            if ( allowsReadPropertyAllRelTypes( propertyKey ) )
            {
                return true;
            }

            if ( disallowsReadPropertyForAllRelTypes( propertyKey ) )
            {
                return false;
            }

            IntSet whitelisted = whitelistedRelTypesForProperty.get( propertyKey );
            IntSet blacklisted = blacklistedRelTypesForProperty.get( propertyKey );

            boolean allowed =
                    (whitelisted != null && whitelisted.contains( relType.getAsInt() ) ) ||
                            whitelistedRelTypesForAllProperties.contains( relType.getAsInt() ) ||
                            allowsReadAllPropertiesAllRelTypes || whitelistedRelationshipProperties.contains( propertyKey );

            boolean disallowedRelType =
                    (blacklisted != null && blacklisted.contains( relType.getAsInt() )) || blacklistedRelTypesForAllProperties.contains( relType.getAsInt() );

            return allowed && !disallowedRelType;
        }

        @Override
        public boolean allowsPropertyReads( int propertyKey )
        {
            if ( disallowsReadAllPropertiesAllLabels && disallowsReadAllPropertiesAllRelTypes )
            {
                return false;
            }

            return !(blacklistedNodeProperties.contains( propertyKey ) && blacklistedRelationshipProperties.contains( propertyKey ));
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

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            if ( passwordChangeRequired )
            {
                return AccessMode.Static.CREDENTIALS_EXPIRED.onViolation( "Permission denied." );
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
            private final String database;

            private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyAccess = new HashMap<>();  // track any access rights
            private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyRead = new HashMap<>();  // track any reads for optimization purposes
            private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyWrite = new HashMap<>(); // track any writes because we only support global write GRANT/DENY
            private boolean token;  // TODO - still to support GRANT/DENY
            private boolean schema; // TODO - still to support GRANT/DENY

            private Map<ResourcePrivilege.GrantOrDeny,Boolean> traverseAllLabels = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,Boolean> traverseAllRelTypes = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> traverseLabels = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> traverseRelTypes = new HashMap<>();

            private Map<ResourcePrivilege.GrantOrDeny,Boolean> readAllPropertiesAllLabels = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,Boolean> readAllPropertiesAllRelTypes = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeSegmentForAllProperties = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipSegmentForAllProperties = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeProperties = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipProperties = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> nodeSegmentForProperty = new HashMap<>();
            private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> relationshipSegmentForProperty = new HashMap<>();

            private StandardAdminAccessMode.Builder adminModeBuilder = new StandardAdminAccessMode.Builder();

            Builder( boolean isAuthenticated, boolean passwordChangeRequired, Set<String> roles, IdLookup resolver, String database )
            {
                this.isAuthenticated = isAuthenticated;
                this.passwordChangeRequired = passwordChangeRequired;
                this.roles = roles;
                this.resolver = resolver;
                this.database = database;
                for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
                {
                    this.traverseLabels.put( privilegeType, IntSets.mutable.empty() );
                    this.traverseRelTypes.put( privilegeType, IntSets.mutable.empty() );
                    this.nodeSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
                    this.relationshipSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
                    this.nodeProperties.put( privilegeType, IntSets.mutable.empty() );
                    this.relationshipProperties.put( privilegeType, IntSets.mutable.empty() );
                    this.nodeSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
                    this.relationshipSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
                }
            }

            private void addBlacklistedPropertyPermissions( MutableIntSet propertyPermissions )
            {
                for ( int property : propertyPermissions.toArray() )
                {
                    nodeProperties.get( DENY ).add( property );
                    relationshipProperties.get( DENY ).add( property );
                }
            }

            StandardAccessMode build()
            {
                return new StandardAccessMode(
                        isAuthenticated && anyAccess.getOrDefault( GRANT, false ),
                        isAuthenticated && anyRead.getOrDefault( GRANT, false ),
                        isAuthenticated && anyWrite.getOrDefault( GRANT, false ) && !anyWrite.getOrDefault( DENY, false ),
                        isAuthenticated && token,
                        isAuthenticated && schema,
                        passwordChangeRequired,
                        roles,

                        traverseAllLabels.getOrDefault( GRANT, false ),
                        traverseAllRelTypes.getOrDefault( GRANT, false ),
                        traverseLabels.get( GRANT ),
                        traverseRelTypes.get( GRANT ),

                        traverseAllLabels.getOrDefault( DENY, false ),
                        traverseAllRelTypes.getOrDefault( DENY, false ),
                        traverseLabels.get( DENY ),
                        traverseRelTypes.get( DENY ),

                        readAllPropertiesAllLabels.getOrDefault( GRANT, false ),
                        readAllPropertiesAllRelTypes.getOrDefault( GRANT, false ),
                        nodeSegmentForAllProperties.get( GRANT ),
                        relationshipSegmentForAllProperties.get( GRANT ),
                        nodeProperties.get( GRANT ),
                        relationshipProperties.get( GRANT ),
                        nodeSegmentForProperty.get( GRANT ),
                        relationshipSegmentForProperty.get( GRANT ),

                        readAllPropertiesAllLabels.getOrDefault( DENY, false ),
                        readAllPropertiesAllRelTypes.getOrDefault( DENY, false ),
                        nodeSegmentForAllProperties.get( DENY ),
                        relationshipSegmentForAllProperties.get( DENY ),
                        nodeProperties.get( DENY ),
                        relationshipProperties.get( DENY ),
                        nodeSegmentForProperty.get( DENY ),
                        relationshipSegmentForProperty.get( DENY ),

                        adminModeBuilder.build(),
                        database );
            }

            void withAccess()
            {
                anyAccess.put( GRANT, true );
            }

            void addPrivilege( ResourcePrivilege privilege ) throws KernelException
            {
                Resource resource = privilege.getResource();
                Segment segment = privilege.getSegment();
                ResourcePrivilege.GrantOrDeny privilegeType = privilege.getPrivilegeType();
                PrivilegeAction action = privilege.getAction();

                switch ( action )
                {
                case ACCESS:
                    anyAccess.put( privilegeType, true );
                    break;

                case TRAVERSE:
                    anyRead.put( privilegeType, true );
                    if ( segment instanceof LabelSegment )
                    {
                        LabelSegment labelSegment = (LabelSegment) segment;
                        if ( labelSegment.equals( LabelSegment.ALL ) )
                        {
                            traverseAllLabels.put( privilegeType, true );
                        }
                        else
                        {
                            addLabel( traverseLabels.get( privilegeType ), labelSegment );
                        }
                    }
                    else if ( segment instanceof RelTypeSegment )
                    {
                        RelTypeSegment relTypeSegment = (RelTypeSegment) segment;
                        if ( relTypeSegment.equals( RelTypeSegment.ALL ) )
                        {
                            traverseAllRelTypes.put( privilegeType, true );
                        }
                        else
                        {
                            addRelType( traverseRelTypes.get( privilegeType ), relTypeSegment );
                        }
                    }
                    else
                    {
                        throw new IllegalStateException( "Unsupported segment qualifier for traverse privilege: " + segment.getClass().getSimpleName() );
                    }
                    break;

                case READ:
                    anyRead.put( privilegeType, true );
                    switch ( resource.type() )
                    {
                    case GRAPH:
                        readAllPropertiesAllLabels.put( privilegeType, true );
                        readAllPropertiesAllRelTypes.put( privilegeType, true );
                        break;
                    case PROPERTY:
                        int propertyId = resolvePropertyId( resource.getArg1() );
                        if ( segment instanceof LabelSegment )
                        {
                            if ( segment.equals( LabelSegment.ALL ) )
                            {
                                nodeProperties.get( privilegeType ).add( propertyId );
                            }
                            else
                            {
                                int labelId = resolveLabelId( ((LabelSegment) segment).getLabel() );
                                addLabelPropertyCombination( nodeSegmentForProperty.get( privilegeType ), labelId, propertyId );
                            }
                        }
                        else if ( segment instanceof RelTypeSegment )
                        {
                            if ( segment.equals( RelTypeSegment.ALL ) )
                            {
                                relationshipProperties.get( privilegeType ).add( propertyId );
                            }
                            else
                            {
                                int relTypeId = resolveRelTypeId( ((RelTypeSegment) segment).getRelType() );
                                addLabelPropertyCombination( relationshipSegmentForProperty.get( privilegeType ), relTypeId, propertyId );
                            }
                        }
                        else
                        {
                            throw new IllegalStateException( "Unsupported segment qualifier for read privilege: " + segment.getClass().getSimpleName() );
                        }
                        break;
                    case ALL_PROPERTIES:
                        if ( segment instanceof LabelSegment )
                        {
                            if ( segment.equals( LabelSegment.ALL ) )
                            {
                                readAllPropertiesAllLabels.put( privilegeType, true );
                            }
                            else
                            {
                                addLabel( nodeSegmentForAllProperties.get( privilegeType ), segment );
                            }
                        }
                        else if ( segment instanceof RelTypeSegment )
                        {
                            RelTypeSegment relTypeSegment = (RelTypeSegment) segment;
                            if ( relTypeSegment.equals( RelTypeSegment.ALL ) )
                            {
                                readAllPropertiesAllRelTypes.put( privilegeType, true );
                            }
                            else
                            {
                                addRelType( relationshipSegmentForAllProperties.get( privilegeType ), relTypeSegment );
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
                        anyWrite.put( privilegeType, true );
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
                    if ( TOKEN.satisfies( action ) )
                    {
                        token = true;
                        addPrivilegeAction( privilege );
                    }
                    else if ( SCHEMA.satisfies( action ) )
                    {
                        schema = true;
                        addPrivilegeAction( privilege );
                    }
                    else if ( ADMIN.satisfies( action ) )
                    {
                        addPrivilegeAction( privilege );
                    }
                }
            }

            private void addPrivilegeAction( ResourcePrivilege privilege )
            {
                var dbScope =
                        privilege.isAllDatabases() ? AdminActionOnResource.DatabaseScope.ALL : new AdminActionOnResource.DatabaseScope( privilege.getDbName() );
                var adminAction = new AdminActionOnResource( privilege.getAction(), dbScope );
                if ( privilege.getPrivilegeType().isGrant() )
                {
                    adminModeBuilder.allow( adminAction );
                }
                else
                {
                    adminModeBuilder.deny( adminAction );
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

            private void addLabel( MutableIntSet whiteOrBlacklist, Segment segment ) throws KernelException
            {
                int labelId = resolveLabelId( ((LabelSegment) segment).getLabel() );
                whiteOrBlacklist.add( labelId );
            }

            private void addRelType( MutableIntSet whiteOrBlacklist, RelTypeSegment segment ) throws KernelException
            {
                whiteOrBlacklist.add( resolveRelTypeId( segment.getRelType() ) );
            }

            private void addLabelPropertyCombination( MutableIntObjectMap<IntSet> map, int labelId, int propertyId )
            {
                MutableIntSet setForProperty = (MutableIntSet) map
                        .getIfAbsentPut( propertyId, IntSets.mutable.empty() );
                setForProperty.add( labelId );
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
