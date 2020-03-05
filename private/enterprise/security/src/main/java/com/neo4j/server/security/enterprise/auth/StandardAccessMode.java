/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.AdminAccessMode;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.LoginContext.IdLookup;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;

import static com.neo4j.server.security.enterprise.auth.Resource.Type.PROPERTY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SCHEMA;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_PROPERTY_KEY;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class StandardAccessMode implements AccessMode
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
    private final IntSet whitelistedNodePropertiesForAllLabels;
    private final IntSet whitelistedNodePropertiesForSomeLabel;
    private final IntSet whitelistedRelationshipPropertiesForAllTypes;
    private final IntSet whitelistedRelationshipPropertiesForSomeType;
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

    private StandardAccessMode(
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
            IntSet whitelistedNodePropertiesForAllLabels,
            IntSet whitelistedRelationshipPropertiesForAllTypes,
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
        this.whitelistedNodePropertiesForAllLabels = whitelistedNodePropertiesForAllLabels;
        this.whitelistedRelationshipPropertiesForAllTypes = whitelistedRelationshipPropertiesForAllTypes;
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

        this.whitelistedNodePropertiesForSomeLabel =
                whitelistedLabelsForProperty.keySet().select( key -> !whitelistedLabelsForProperty.get( key ).isEmpty() );

        this.whitelistedRelationshipPropertiesForSomeType =
                whitelistedRelTypesForProperty.keySet().select( key -> !whitelistedRelTypesForProperty.get( key ).isEmpty() );
    }

    @Override
    public boolean allowsWrites()
    {
        return allowsWrites;
    }

    @Override
    public boolean allowsTokenCreates( PrivilegeAction action )
    {
        return allowsTokenCreates && adminAccessMode.allows( new AdminActionOnResource( action, database, Segment.ALL ) );
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return allowsSchemaWrites;
    }

    @Override
    public boolean allowsSchemaWrites( PrivilegeAction action )
    {
        return allowsSchemaWrites && adminAccessMode.allows( new AdminActionOnResource( action, database, Segment.ALL ) );
    }

    @Override
    public boolean allowsTraverseAllLabels()
    {
        return allowsTraverseAllLabels && !disallowsTraverseAllLabels && blacklistTraverseLabels.isEmpty();
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel( long label )
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
    public boolean allowsTraverseNode( long... labels )
    {
        if ( allowsTraverseAllLabels() )
        {
            return true;
        }
        if ( disallowsTraverseAllLabels || labels.length == 1 && labels[0] == ANY_LABEL )
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
        if ( relType == ANY_RELATIONSHIP_TYPE || disallowsTraverseAllRelTypes || blacklistTraverseRelTypes.contains( relType ) )
        {
            return false;
        }

        return allowsTraverseAllRelTypes || whitelistTraverseRelTypes.contains( relType );
    }

    @Override
    public boolean allowsReadPropertyAllLabels( int propertyKey )
    {
        return (allowsReadAllPropertiesAllLabels || whitelistedNodePropertiesForAllLabels.contains( propertyKey )) &&
                !disallowsReadPropertyForSomeLabel( propertyKey );
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
        return allowed || allowsReadAllPropertiesAllLabels || whitelistedNodePropertiesForAllLabels.contains( propertyKey );
    }

    @Override
    public boolean allowsReadPropertyAllRelTypes( int propertyKey )
    {
        return (allowsReadAllPropertiesAllRelTypes || whitelistedRelationshipPropertiesForAllTypes.contains( propertyKey )) &&
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
                        allowsReadAllPropertiesAllRelTypes || whitelistedRelationshipPropertiesForAllTypes.contains( propertyKey );

        boolean disallowedRelType =
                (blacklisted != null && blacklisted.contains( relType.getAsInt() )) || blacklistedRelTypesForAllProperties.contains( relType.getAsInt() );

        return allowed && !disallowedRelType;
    }

    @Override
    public boolean allowsSeePropertyKeyToken( int propertyKey )
    {
        boolean disabledForNodes =
                disallowsReadAllPropertiesAllLabels || blacklistedNodeProperties.contains( propertyKey ) || !allowPropertyReadOnSomeNode( propertyKey );

        boolean disabledForRels = disallowsReadAllPropertiesAllRelTypes || blacklistedRelationshipProperties.contains( propertyKey ) ||
                !allowsPropertyReadOnSomeRelType( propertyKey );

        return !(disabledForNodes && disabledForRels);
    }

    private boolean allowPropertyReadOnSomeNode( int propertyKey )
    {
        return allowsReadAllPropertiesAllLabels || whitelistedNodePropertiesForAllLabels.contains( propertyKey ) ||
                whitelistedNodePropertiesForSomeLabel.contains( propertyKey ) || whitelistedLabelsForAllProperties.notEmpty();
    }

    private boolean allowsPropertyReadOnSomeRelType( int propertyKey )
    {
        return allowsReadAllPropertiesAllRelTypes || whitelistedRelationshipPropertiesForAllTypes.contains( propertyKey ) ||
                whitelistedRelationshipPropertiesForSomeType.contains( propertyKey ) || whitelistedRelTypesForAllProperties.notEmpty();
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
            return Static.CREDENTIALS_EXPIRED.onViolation( "Permission denied." );
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
        return roles.isEmpty() ? "no roles" : "roles " + sortedRoles.toString();
    }

    boolean allowsAccess()
    {
        return allowsAccess;
    }

    Set<String> getRoles()
    {
        return roles;
    }

    AdminAccessMode getAdminAccessMode()
    {
        return adminAccessMode;
    }

    static class Builder
    {
        private final boolean isAuthenticated;
        private final boolean passwordChangeRequired;
        private final Set<String> roles;
        private final IdLookup resolver;
        private final String database;
        private final String defaultDbName;

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

        Builder( boolean isAuthenticated, boolean passwordChangeRequired, Set<String> roles, IdLookup resolver, String database, String defaultDbName )
        {
            this.isAuthenticated = isAuthenticated;
            this.passwordChangeRequired = passwordChangeRequired;
            this.roles = roles;
            this.resolver = resolver;
            this.database = database;
            this.defaultDbName = defaultDbName;
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

        StandardAccessMode build()
        {
            return new StandardAccessMode(
                    isAuthenticated && anyAccess.getOrDefault( GRANT, false ) && !anyAccess.getOrDefault( DENY, false ),
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

        Builder withAccess()
        {
            anyAccess.put( GRANT, true );
            return this;
        }

        Builder addPrivilege( ResourcePrivilege privilege )
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
                handleTraversePrivilege( segment, privilegeType, "traverse" );
                break;

            case READ:
                anyRead.put( privilegeType, true );
                handleReadPrivilege( resource, segment, privilegeType, "read" );
                break;

            case MATCH:
                anyRead.put( privilegeType, true );
                if ( !(privilegeType.isDeny() && resource.type() == PROPERTY) )
                {
                    // don't deny TRAVERSE for DENY MATCH {prop}
                    handleTraversePrivilege( segment, privilegeType, "match" );
                }
                handleReadPrivilege( resource, segment, privilegeType, "match" );
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
                else if ( DATABASE_ACTIONS.satisfies( action ) )
                {
                    anyAccess.put( privilegeType, true );
                    token = true;
                    schema = true;
                    addPrivilegeAction( privilege );
                }
            }
            return this;
        }

        private void handleTraversePrivilege( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName )
        {
            if ( segment instanceof LabelSegment )
            {
                if ( segment.equals( LabelSegment.ALL ) )
                {
                    traverseAllLabels.put( privilegeType, true );
                }
                else
                {
                    addLabel( traverseLabels.get( privilegeType ), (LabelSegment) segment );
                }
            }
            else if ( segment instanceof RelTypeSegment )
            {
                if ( segment.equals( RelTypeSegment.ALL ) )
                {
                    traverseAllRelTypes.put( privilegeType, true );
                }
                else
                {
                    addRelType( traverseRelTypes.get( privilegeType ), (RelTypeSegment) segment );
                }
            }
            else
            {
                throw new IllegalStateException( "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
            }
        }

        private void handleReadPrivilege( Resource resource, Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName )
        {
            switch ( resource.type() )
            {
            case GRAPH:
                readAllPropertiesAllLabels.put( privilegeType, true );
                readAllPropertiesAllRelTypes.put( privilegeType, true );
                break;
            case PROPERTY:
                int propertyId = resolvePropertyId( resource.getArg1() );
                if ( propertyId == ANY_PROPERTY_KEY )
                {
                    // there exists no property with this name at the start of this transaction
                    break;
                }
                if ( segment instanceof LabelSegment )
                {
                    if ( segment.equals( LabelSegment.ALL ) )
                    {
                        nodeProperties.get( privilegeType ).add( propertyId );
                    }
                    else
                    {
                        addLabel( nodeSegmentForProperty.get( privilegeType ), (LabelSegment) segment, propertyId );
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
                        addRelType( relationshipSegmentForProperty.get( privilegeType ), (RelTypeSegment) segment, propertyId );
                    }
                }
                else
                {
                    throw new IllegalStateException( "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
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
                        addLabel( nodeSegmentForAllProperties.get( privilegeType ), (LabelSegment) segment );
                    }
                }
                else if ( segment instanceof RelTypeSegment )
                {
                    if ( segment.equals( RelTypeSegment.ALL ) )
                    {
                        readAllPropertiesAllRelTypes.put( privilegeType, true );
                    }
                    else
                    {
                        addRelType( relationshipSegmentForAllProperties.get( privilegeType ), (RelTypeSegment) segment );
                    }
                }
                else
                {
                    throw new IllegalStateException( "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
                }
                break;
            default:
            }
        }

        private void addPrivilegeAction( ResourcePrivilege privilege )
        {
            AdminActionOnResource.DatabaseScope dbScope;
            if ( privilege.appliesToAll() )
            {
                dbScope = AdminActionOnResource.DatabaseScope.ALL;
            }
            else if ( privilege.appliesToDefault() )
            {
                dbScope = new AdminActionOnResource.DatabaseScope( defaultDbName );
            }
            else
            {
                dbScope = new AdminActionOnResource.DatabaseScope( privilege.getDbName() );
            }
            var adminAction = new AdminActionOnResource( privilege.getAction(), dbScope, privilege.getSegment() );
            if ( privilege.getPrivilegeType().isGrant() )
            {
                adminModeBuilder.allow( adminAction );
            }
            else
            {
                adminModeBuilder.deny( adminAction );
            }
        }

        private int resolveLabelId( String label )
        {
            assert !label.isEmpty();
            return resolver.getLabelId( label );
        }

        private int resolveRelTypeId( String relType )
        {
            assert !relType.isEmpty();
            return resolver.getRelTypeId( relType );
        }

        private int resolvePropertyId( String property )
        {
            assert !property.isEmpty();
            return resolver.getPropertyKeyId( property );
        }

        private void addLabel( MutableIntObjectMap<IntSet> map, LabelSegment segment, int propertyId )
        {
            // propertyId has already been checked that it is not ANY_LABEL
            MutableIntSet setForProperty = (MutableIntSet) map.getIfAbsentPut( propertyId, IntSets.mutable.empty() );
            addLabel( setForProperty, segment );
        }

        private void addLabel( MutableIntSet whiteOrBlacklist, LabelSegment segment )
        {
            int labelId = resolveLabelId( segment.getLabel() );
            if ( labelId != ANY_LABEL )
            {
                whiteOrBlacklist.add( labelId );
            }
        }

        private void addRelType( MutableIntObjectMap<IntSet> map, RelTypeSegment segment, int propertyId )
        {
            // propertyId has already been checked that it is not ANY_RELATIONSHIP_TYPE
            MutableIntSet setForProperty = (MutableIntSet) map.getIfAbsentPut( propertyId, IntSets.mutable.empty() );
            addRelType( setForProperty, segment );
        }

        private void addRelType( MutableIntSet whiteOrBlacklist, RelTypeSegment segment )
        {
            int relTypeId = resolveRelTypeId( segment.getRelType() );
            if ( relTypeId != ANY_RELATIONSHIP_TYPE )
            {
                whiteOrBlacklist.add( relTypeId );
            }
        }
    }
}
