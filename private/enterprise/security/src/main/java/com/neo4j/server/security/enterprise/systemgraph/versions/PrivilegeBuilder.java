/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import org.neo4j.internal.kernel.api.security.LabelSegment;
import org.neo4j.internal.kernel.api.security.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.internal.helpers.collection.Iterables.single;

@SuppressWarnings( "UnusedReturnValue" )
public class PrivilegeBuilder
{
    final ResourcePrivilege.GrantOrDeny privilegeType;
    Segment segment;
    Set<PrivilegeAction> actions;
    Resource resource;
    String dbName = "";
    SpecialDatabase specialDatabase;

    public PrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        this( privilegeType, action, a -> Collections.singleton( PrivilegeAction.valueOf( a ) ) );
    }

    PrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action, Function<String, Set<PrivilegeAction>> actionMapper )
    {
        this.privilegeType = privilegeType;
        this.actions = actionMapper.apply( action.toUpperCase() );
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
        case "ProcedureQualifier":
            String procedureQualifier = qualifierNode.getProperty( "label" ).toString();
            this.segment = new ProcedureSegment( procedureQualifier );
            break;
        case "ProcedureQualifierAll":
            this.segment = ProcedureSegment.ALL;
            break;
        case "FunctionQualifier":
            String functionQualifier = qualifierNode.getProperty( "label" ).toString();
            this.segment = new FunctionSegment( functionQualifier );
            break;
        case "FunctionQualifierAll":
            this.segment = FunctionSegment.ALL;
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

    Set<ResourcePrivilege> build() throws InvalidArgumentsException
    {
        Set<ResourcePrivilege> privileges = new HashSet<>();
        for ( PrivilegeAction action : actions )
        {
            if ( specialDatabase != null )
            {
                privileges.add( new ResourcePrivilege( privilegeType, action, resource, segment, specialDatabase ) );
            }
            else
            {
                privileges.add( new ResourcePrivilege( privilegeType, action, resource, segment, dbName ) );
            }
        }
        return privileges;
    }
}
