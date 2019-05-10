/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.Segment;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.NodeValue;

import static org.neo4j.helpers.collection.MapUtil.map;

class PrivilegeBuilder
{
    private final boolean allowed;
    private QueryExecutor queryExecutor;
    private Set<String> labels = new HashSet<>();
    private ResourcePrivilege.Action action;
    private Resource resource;

    private PrivilegeBuilder( boolean allowed, QueryExecutor queryExecutor, String action )
    {
        this.allowed = allowed;
        this.queryExecutor = queryExecutor;
        this.action = ResourcePrivilege.Action.valueOf( action.toUpperCase() );
    }

    static PrivilegeBuilder grant( QueryExecutor queryExecutor, String action )
    {
        return new PrivilegeBuilder( true, queryExecutor, action );
    }

    PrivilegeBuilder withinScope( NodeValue segment )
    {
        queryExecutor.executeQuery( "MATCH (s:Segment)-[:QUALIFIED]->(q) WHERE id(s) = $nodeId RETURN q",
                map( "nodeId", segment.id() ), row ->
                {
                    NodeValue qualifier = (NodeValue) row.fields()[0];
                    switch ( qualifier.labels().stringValue( 0 ) )
                    {
                    case "LabelQualifier":
                        this.labels.add( ((TextValue) qualifier.properties().get( "label" )).stringValue() );
                        break;
                    default:
                        throw new InvalidArgumentsException( "Unknown privilege qualifier type: " + qualifier.labels().stringValue( 0 ) );
                    }
                    return true;
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
        Segment segment = labels.isEmpty() ? Segment.ALL : new Segment( labels );
        return new ResourcePrivilege( action, resource, segment );
    }
}
