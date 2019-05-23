/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.Segment;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.NodeValue;

class PrivilegeBuilder
{
    private final boolean allowed;
    private String label;
    private ResourcePrivilege.Action action;
    private Resource resource;

    private PrivilegeBuilder( boolean allowed, String action )
    {
        this.allowed = allowed;
        this.action = ResourcePrivilege.Action.valueOf( action.toUpperCase() );
    }

    static PrivilegeBuilder grant( String action )
    {
        return new PrivilegeBuilder( true, action );
    }

    PrivilegeBuilder withinScope( NodeValue qualifier )
    {
        if ( qualifier.labels().length() != 1 )
        {
            throw new IllegalStateException(
                    "Privilege segments require qualifier nodes with exactly one label, but this qualifier has: " + qualifier.labels().prettyPrint() );
        }
        qualifier.labels().forEach( label ->
        {
            switch ( ((StringValue)label).stringValue() )
            {
            case "LabelQualifier":
                this.label = ((TextValue) qualifier.properties().get( "label" )).stringValue();
                break;
            case "LabelQualifierAll":
                this.label = null;
                break;
            default:
                throw new IllegalArgumentException( "Unknown privilege qualifier type: " + label.getTypeName() );
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
        Segment segment = label == null ? Segment.ALL : new Segment( label );
        return new ResourcePrivilege( action, resource, segment );
    }
}
