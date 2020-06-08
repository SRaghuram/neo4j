/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;

import static com.neo4j.server.security.enterprise.auth.Resource.Type.ALL_LABELS;
import static com.neo4j.server.security.enterprise.auth.Resource.Type.PROPERTY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_PROPERTY_KEY;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class StandardAccessModeBuilder
{
    private final boolean isAuthenticated;
    private final boolean passwordChangeRequired;
    private final Set<String> roles;
    private final LoginContext.IdLookup resolver;
    private final String database;
    private final String defaultDbName;

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyAccess = new HashMap<>();  // track any access rights
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyRead = new HashMap<>();  // track any reads for optimization purposes
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> anyWrite = new HashMap<>(); // track any writes
    private boolean token;  // TODO - still to support GRANT/DENY
    private boolean schema; // TODO - still to support GRANT/DENY

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> traverseAllLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> traverseAllRelTypes = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> traverseLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> traverseRelTypes = new HashMap<>();

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> setAllLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> settableLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> removeAllLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> removableLabels = new HashMap<>();

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> createNodeWithAnyLabel = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> createNodeWithLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> deleteNodeWithAnyLabel = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> deleteNodeWithLabels = new HashMap<>();

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> createRelationshipWithAnyType = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> createRelationshipWithTypes = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> deleteRelationshipWithAnyType = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> deleteRelationshipWithTypes = new HashMap<>();

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> readAllPropertiesAllLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> readAllPropertiesAllRelTypes = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeSegmentForAllProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipSegmentForAllProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> nodeSegmentForProperty = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> relationshipSegmentForProperty = new HashMap<>();

    private Map<ResourcePrivilege.GrantOrDeny,Boolean> writeAllPropertiesAllLabels = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,Boolean> writeAllPropertiesAllRelTypes = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeNodeSegmentForAllProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeRelationshipSegmentForAllProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeNodeProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> writeRelationshipProperties = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> writeNodeSegmentForProperty = new HashMap<>();
    private Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> writeRelationshipSegmentForProperty = new HashMap<>();

    private StandardAdminAccessMode.Builder adminModeBuilder = new StandardAdminAccessMode.Builder();

    StandardAccessModeBuilder( boolean isAuthenticated, boolean passwordChangeRequired, Set<String> roles, LoginContext.IdLookup resolver, String database,
                               String defaultDbName )
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
            this.settableLabels.put( privilegeType, IntSets.mutable.empty() );
            this.removableLabels.put( privilegeType, IntSets.mutable.empty() );
            this.createNodeWithLabels.put( privilegeType, IntSets.mutable.empty() );
            this.deleteNodeWithLabels.put( privilegeType, IntSets.mutable.empty() );
            this.createRelationshipWithTypes.put( privilegeType, IntSets.mutable.empty() );
            this.deleteRelationshipWithTypes.put( privilegeType, IntSets.mutable.empty() );
            this.writeNodeSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
            this.writeRelationshipSegmentForAllProperties.put( privilegeType, IntSets.mutable.empty() );
            this.writeNodeProperties.put( privilegeType, IntSets.mutable.empty() );
            this.writeRelationshipProperties.put( privilegeType, IntSets.mutable.empty() );
            this.writeNodeSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
            this.writeRelationshipSegmentForProperty.put( privilegeType, IntObjectMaps.mutable.empty() );
        }
    }

    StandardAccessMode build()
    {
        return new StandardAccessMode(
                isAuthenticated && anyAccess.getOrDefault( GRANT, false ) && !anyAccess.getOrDefault( DENY, false ),
                isAuthenticated && anyRead.getOrDefault( GRANT, false ),
                isAuthenticated && anyWrite.getOrDefault( GRANT, false ),
                anyWrite.getOrDefault( DENY, false ),
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

                new PropertyPrivileges( readAllPropertiesAllLabels.getOrDefault( GRANT, false ),
                                        readAllPropertiesAllRelTypes.getOrDefault( GRANT, false ),
                                        nodeSegmentForAllProperties.get( GRANT ),
                                        relationshipSegmentForAllProperties.get( GRANT ),
                                        nodeProperties.get( GRANT ),
                                        relationshipProperties.get( GRANT ),
                                        nodeSegmentForProperty.get( GRANT ),
                                        relationshipSegmentForProperty.get( GRANT ) ),

                new PropertyPrivileges( readAllPropertiesAllLabels.getOrDefault( DENY, false ),
                                        readAllPropertiesAllRelTypes.getOrDefault( DENY, false ),
                                        nodeSegmentForAllProperties.get( DENY ),
                                        relationshipSegmentForAllProperties.get( DENY ),
                                        nodeProperties.get( DENY ),
                                        relationshipProperties.get( DENY ),
                                        nodeSegmentForProperty.get( DENY ),
                                        relationshipSegmentForProperty.get( DENY ) ),

                setAllLabels.getOrDefault( GRANT, false ),
                settableLabels.get( GRANT ),
                setAllLabels.getOrDefault( DENY, false ),
                settableLabels.get( DENY ),
                removeAllLabels.getOrDefault( GRANT, false ),
                removableLabels.get( GRANT ),
                removeAllLabels.getOrDefault( DENY, false ),
                removableLabels.get( DENY ),

                createNodeWithAnyLabel.getOrDefault( GRANT, false ),
                createNodeWithLabels.get( GRANT ),
                createNodeWithAnyLabel.getOrDefault( DENY, false ),
                createNodeWithLabels.get( DENY ),
                deleteNodeWithAnyLabel.getOrDefault( GRANT, false ),
                deleteNodeWithLabels.get( GRANT ),
                deleteNodeWithAnyLabel.getOrDefault( DENY, false ),
                deleteNodeWithLabels.get( DENY ),

                createRelationshipWithAnyType.getOrDefault( GRANT, false ),
                createRelationshipWithTypes.get( GRANT ),
                createRelationshipWithAnyType.getOrDefault( DENY, false ),
                createRelationshipWithTypes.get( DENY ),
                deleteRelationshipWithAnyType.getOrDefault( GRANT, false ),
                deleteRelationshipWithTypes.get( GRANT ),
                deleteRelationshipWithAnyType.getOrDefault( DENY, false ),
                deleteRelationshipWithTypes.get( DENY ),

                new PropertyPrivileges( writeAllPropertiesAllLabels.getOrDefault( GRANT, false ),
                                        writeAllPropertiesAllRelTypes.getOrDefault( GRANT, false ),
                                        writeNodeSegmentForAllProperties.get( GRANT ),
                                        writeRelationshipSegmentForAllProperties.get( GRANT ),
                                        writeNodeProperties.get( GRANT ),
                                        writeRelationshipProperties.get( GRANT ),
                                        writeNodeSegmentForProperty.get( GRANT ),
                                        writeRelationshipSegmentForProperty.get( GRANT ) ),

                new PropertyPrivileges( writeAllPropertiesAllLabels.getOrDefault( DENY, false ),
                                        writeAllPropertiesAllRelTypes.getOrDefault( DENY, false ),
                                        writeNodeSegmentForAllProperties.get( DENY ),
                                        writeRelationshipSegmentForAllProperties.get( DENY ),
                                        writeNodeProperties.get( DENY ),
                                        writeRelationshipProperties.get( DENY ),
                                        writeNodeSegmentForProperty.get( DENY ),
                                        writeRelationshipSegmentForProperty.get( DENY ) ),

                adminModeBuilder.build(),
                database );
    }

    StandardAccessModeBuilder withAccess()
    {
        anyAccess.put( GRANT, true );
        return this;
    }

    StandardAccessModeBuilder addPrivilege( ResourcePrivilege privilege )
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
            handleAllResourceQualifier( segment, privilegeType, "traverse", traverseAllLabels, traverseLabels, traverseAllRelTypes, traverseRelTypes );
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
                handleAllResourceQualifier( segment, privilegeType, "match", traverseAllLabels, traverseLabels, traverseAllRelTypes, traverseRelTypes );
            }
            handleReadPrivilege( resource, segment, privilegeType, "match" );
            break;

        case WRITE:
            anyWrite.put( privilegeType, true );
            break;

        case SET_LABEL:
        case REMOVE_LABEL:
            handleLabelPrivilege( resource, privilegeType, action );
            break;

        case CREATE_ELEMENT:
            handleCreatePrivilege( segment, privilegeType );
            break;

        case DELETE_ELEMENT:
            handleDeletePrivilege( segment, privilegeType );
            break;
        case SET_PROPERTY:
            handleSetPropertyPrivilege( resource, segment, privilegeType );
            break;
        default:
            if ( TOKEN.satisfies( action ) )
            {
                token = true;
                addPrivilegeAction( privilege );
            }
            else if ( INDEX.satisfies( action ) || CONSTRAINT.satisfies( action ) )
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

    private void handleCreatePrivilege( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType )
    {
        if ( segment instanceof LabelSegment )
        {
            if ( segment.equals( LabelSegment.ALL ) )
            {
                createNodeWithAnyLabel.put( privilegeType, true );
            }
            else
            {
                addLabel( createNodeWithLabels.get( privilegeType ), (LabelSegment) segment );
            }
        }
        else if ( segment instanceof RelTypeSegment )
        {
            if ( segment.equals( RelTypeSegment.ALL ) )
            {
                createRelationshipWithAnyType.put( privilegeType, true );
            }
            else
            {
                addRelType( createRelationshipWithTypes.get( privilegeType ), (RelTypeSegment) segment );
            }
        }
        else
        {
            throw new IllegalStateException( "Unsupported segment qualifier for create privilege: " + segment.getClass().getSimpleName() );
        }
    }

    private void handleDeletePrivilege( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType )
    {
        if ( segment instanceof LabelSegment )
        {
            if ( segment.equals( LabelSegment.ALL ) )
            {
                deleteNodeWithAnyLabel.put( privilegeType, true );
            }
            else
            {
                addLabel( deleteNodeWithLabels.get( privilegeType ), (LabelSegment) segment );
            }
        }
        else if ( segment instanceof RelTypeSegment )
        {
            if ( segment.equals( RelTypeSegment.ALL ) )
            {
                deleteRelationshipWithAnyType.put( privilegeType, true );
            }
            else
            {
                addRelType( deleteRelationshipWithTypes.get( privilegeType ), (RelTypeSegment) segment );
            }
        }
        else
        {
            throw new IllegalStateException( "Unsupported segment qualifier for delete privilege: " + segment.getClass().getSimpleName() );
        }
    }

    private void handleLabelPrivilege( Resource resource, ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action )
    {
        if ( resource.type().equals( ALL_LABELS ) )
        {
            if ( action == SET_LABEL )
            {
                setAllLabels.put( privilegeType, true );
            }
            else if ( action == REMOVE_LABEL )
            {
                removeAllLabels.put( privilegeType, true );
            }
        }
        else
        {
            if ( action == SET_LABEL )
            {
                addLabelResource( settableLabels.get( privilegeType ), (Resource.LabelResource) resource );
            }
            else if ( action == REMOVE_LABEL )
            {
                addLabelResource( removableLabels.get( privilegeType ), (Resource.LabelResource) resource );
            }
        }
    }

    private void handleSetPropertyPrivilege( Resource resource, Segment segment, ResourcePrivilege.GrantOrDeny privilegeType )
    {
        switch ( resource.type() )
        {
        case PROPERTY:
            handlePropertyResource( resource, segment, privilegeType, "set property", writeNodeProperties, writeNodeSegmentForProperty,
                                    writeRelationshipProperties, writeRelationshipSegmentForProperty );
            break;
        case ALL_PROPERTIES:
            handleAllResourceQualifier( segment, privilegeType, "set property", writeAllPropertiesAllLabels, writeNodeSegmentForAllProperties,
                                        writeAllPropertiesAllRelTypes, writeRelationshipSegmentForAllProperties );
            break;
        default:
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
            handlePropertyResource( resource, segment, privilegeType, privilegeName, nodeProperties, nodeSegmentForProperty, relationshipProperties,
                                    relationshipSegmentForProperty );
            break;
        case ALL_PROPERTIES:
            handleAllResourceQualifier( segment, privilegeType, privilegeName, readAllPropertiesAllLabels, nodeSegmentForAllProperties,
                                        readAllPropertiesAllRelTypes, relationshipSegmentForAllProperties );
            break;
        default:
        }
    }

    private void handlePropertyResource( Resource propertyResource, Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName,
                                         Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeProperties,
                                         Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> nodeSegmentForProperty,
                                         Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipProperties,
                                         Map<ResourcePrivilege.GrantOrDeny,MutableIntObjectMap<IntSet>> relationshipSegmentForProperty )
    {
        int propertyId = resolvePropertyId( propertyResource.getArg1() );
        if ( propertyId == ANY_PROPERTY_KEY )
        {
            return;
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
            throw new IllegalStateException(
                    "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
        }
    }

    private void handleAllResourceQualifier( Segment segment, ResourcePrivilege.GrantOrDeny privilegeType, String privilegeName,
                                             Map<ResourcePrivilege.GrantOrDeny,Boolean> allPropertiesAllLabels,
                                             Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> nodeSegmentForAllProperties,
                                             Map<ResourcePrivilege.GrantOrDeny,Boolean> allPropertiesAllRelTypes,
                                             Map<ResourcePrivilege.GrantOrDeny,MutableIntSet> relationshipSegmentForAllProperties )
    {
        if ( segment instanceof LabelSegment )
        {
            if ( segment.equals( LabelSegment.ALL ) )
            {
                allPropertiesAllLabels.put( privilegeType, true );
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
                allPropertiesAllRelTypes.put( privilegeType, true );
            }
            else
            {
                addRelType( relationshipSegmentForAllProperties.get( privilegeType ), (RelTypeSegment) segment );
            }
        }
        else
        {
            throw new IllegalStateException(
                    "Unsupported segment qualifier for " + privilegeName + " privilege: " + segment.getClass().getSimpleName() );
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

    private void addLabelResource( MutableIntSet whiteOrBlacklist, Resource.LabelResource resource )
    {
        int labelId = resolveLabelId( resource.getArg1() );
        if ( labelId != ANY_LABEL )
        {
            whiteOrBlacklist.add( labelId );
        }
    }
}
