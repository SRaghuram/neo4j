/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabaseSegment;
import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.Segment;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.NodeValue;

class PrivilegeBuilder
{
    private final ResourcePrivilege.GrantOrDeny privilegeType;
    private Segment segment;
    private ResourcePrivilege.Action action;
    private Resource resource;
    private String dbName = "";
    private boolean allDatabases;

    PrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        this.privilegeType = privilegeType;
        this.action = ResourcePrivilege.Action.valueOf( action.toUpperCase() );
    }

    PrivilegeBuilder forAllDatabases()
    {
        this.allDatabases = true;
        return this;
    }

    PrivilegeBuilder forDatabase( String database )
    {
        this.dbName = database;
        return this;
    }

    PrivilegeBuilder withinScope( NodeValue qualifierNode )
    {
        if ( qualifierNode.labels().length() != 1 )
        {
            throw new IllegalStateException(
                    "Privilege segments require qualifier nodes with exactly one label, but this qualifier has: " + qualifierNode.labels().prettyPrint() );
        }
        qualifierNode.labels().forEach( qualifierType ->
        {
            switch ( ((StringValue) qualifierType).stringValue() )
            {
            case "DatabaseQualifier":
                this.segment = DatabaseSegment.ALL;
                break;
            case "LabelQualifier":
                String label = ((TextValue) qualifierNode.properties().get( "label" )).stringValue();
                this.segment = new LabelSegment( label );
                break;
            case "LabelQualifierAll":
                this.segment = LabelSegment.ALL;
                break;
            case "RelationshipQualifier":
                String relType = ((TextValue) qualifierNode.properties().get( "label" )).stringValue();
                this.segment = new RelTypeSegment( relType );
                break;
            case "RelationshipQualifierAll":
                this.segment = RelTypeSegment.ALL;
                break;
            default:
                throw new IllegalArgumentException( "Unknown privilege qualifier type: " + qualifierType.getTypeName() );
            }
        } );
        return this;
    }

    PrivilegeBuilder onResource( NodeValue resource ) throws InvalidArgumentsException
    {
        String type = ((TextValue) resource.properties().get( "type" )).stringValue();
        Resource.Type resourceType = asResourceType( type );
        switch ( resourceType )
        {
        case DATABASE:
            this.resource = new Resource.DatabaseResource();
            break;
        case GRAPH:
            this.resource = new Resource.GraphResource();
            break;
        case PROPERTY:
            String propertyKey = ((TextValue) resource.properties().get( "arg1" )).stringValue();
            this.resource = new Resource.PropertyResource( propertyKey );
            break;
        case ALL_PROPERTIES:
            this.resource = new Resource.AllPropertiesResource();
            break;
        case TOKEN:
            this.resource = new Resource.TokenResource();
            break;
        case SCHEMA:
            this.resource = new Resource.SchemaResource();
            break;
        case SYSTEM:
            this.resource = new Resource.SystemResource();
            break;
        case PROCEDURE:
            String namespace = ((TextValue) resource.properties().get( "arg1" )).stringValue();
            String procedureName = ((TextValue) resource.properties().get( "arg2" )).stringValue();
            this.resource = new Resource.ProcedureResource( namespace, procedureName );
            break;
        default:
            throw new IllegalArgumentException( "Unknown resourceType: " + resourceType );
        }
        return this;
    }

    private Resource.Type asResourceType( String typeString ) throws InvalidArgumentsException
    {
        try
        {
            return Resource.Type.valueOf( typeString.toUpperCase() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new InvalidArgumentsException( String.format( "Found not valid resource (%s) in the system graph.", typeString ) );
        }
    }

    ResourcePrivilege build() throws InvalidArgumentsException
    {
        if ( allDatabases )
        {
            return new ResourcePrivilege( privilegeType, action, resource, segment );
        }
        else
        {
            return new ResourcePrivilege( privilegeType, action, resource, segment, dbName );
        }
    }
}
