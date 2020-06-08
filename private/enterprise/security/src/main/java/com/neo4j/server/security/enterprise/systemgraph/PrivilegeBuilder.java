/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.internal.helpers.collection.Iterables.single;

class PrivilegeBuilder
{
    private final ResourcePrivilege.GrantOrDeny privilegeType;
    private Segment segment;
    private PrivilegeAction action;
    private Resource resource;
    private String dbName = "";
    private SpecialDatabase specialDatabase;

    PrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        this.privilegeType = privilegeType;
        this.action = PrivilegeAction.valueOf( action.toUpperCase() );
    }

    PrivilegeBuilder forAllDatabases()
    {
        this.specialDatabase = SpecialDatabase.ALL;
        return this;
    }

    PrivilegeBuilder forDefaultDatabase()
    {
        this.specialDatabase = SpecialDatabase.DEFAULT;
        return this;
    }

    PrivilegeBuilder forDatabase( String database )
    {
        this.dbName = database;
        return this;
    }

    PrivilegeBuilder withinScope( Node qualifierNode )
    {
        Label qualifierType;
        try
        {
            qualifierType = single( qualifierNode.getLabels() );
        }
        catch ( NoSuchElementException e )
        {
            throw new IllegalStateException( "Privilege segments require qualifier nodes with exactly one label. " + e.getMessage() );
        }
        switch ( qualifierType.name() )
        {
        case "DatabaseQualifier":
            this.segment = Segment.ALL;
            break;
        case "LabelQualifier":
            String label = qualifierNode.getProperty( "label" ).toString();
            this.segment = new LabelSegment( label );
            break;
        case "LabelQualifierAll":
            this.segment = LabelSegment.ALL;
            break;
        case "RelationshipQualifier":
            String relType = qualifierNode.getProperty( "label" ).toString();
            this.segment = new RelTypeSegment( relType );
            break;
        case "RelationshipQualifierAll":
            this.segment = RelTypeSegment.ALL;
            break;
        case "UserQualifier":
            String username = qualifierNode.getProperty( "label" ).toString();
            this.segment = new UserSegment( username );
            break;
        case "UserQualifierAll":
            this.segment = UserSegment.ALL;
            break;
        default:
            throw new IllegalArgumentException( "Unknown privilege qualifier type: " + qualifierType.name() );
        }
        return this;
    }

    PrivilegeBuilder onResource( Node resourceNode ) throws InvalidArgumentsException
    {
        String type = resourceNode.getProperty( "type" ).toString();
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
            String propertyKey = resourceNode.getProperty( "arg1" ).toString();
            this.resource = new Resource.PropertyResource( propertyKey );
            break;
        case ALL_PROPERTIES:
            this.resource = new Resource.AllPropertiesResource();
            break;
        case LABEL:
            String label = resourceNode.getProperty( "arg1" ).toString();
            this.resource = new Resource.LabelResource( label );
            break;
        case ALL_LABELS:
            this.resource = new Resource.AllLabelsResource();
            break;
        case PROCEDURE:
            String namespace = resourceNode.getProperty( "arg1" ).toString();
            String procedureName = resourceNode.getProperty( "arg2" ).toString();
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
        if ( specialDatabase != null )
        {
            return new ResourcePrivilege( privilegeType, action, resource, segment, specialDatabase );
        }
        else
        {
            return new ResourcePrivilege( privilegeType, action, resource, segment, dbName );
        }
    }
}
