/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.NodeValue;

class PrivilegeBuilder
{
    private final boolean allowed;
    private QueryExecutor queryExecutor;
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

    PrivilegeBuilder onResource( NodeValue resource )
    {
        String type = ((TextValue) resource.properties().get( "type" )).stringValue();
        switch ( type.toUpperCase() )
        {
        case "GRAPH":
            this.resource = new Resource.GraphResource();
            break;
        case "LABEL":
            String label = ((TextValue) resource.properties().get( "arg1" )).stringValue();
            String property = ((TextValue) resource.properties().get( "arg2" )).stringValue();
            this.resource = new Resource.LabelResource( label, property );
            break;
        case "TOKEN":
            this.resource = new Resource.TokenResource();
            break;
        case "SCHEMA":
            this.resource = new Resource.SchemaResource();
            break;
        case "SYSTEM":
            this.resource = new Resource.SystemResource();
            break;
        case "PROCEDURE":
            String namespace = ((TextValue) resource.properties().get( "arg1" )).stringValue();
            String procedureName = ((TextValue) resource.properties().get( "arg2" )).stringValue();
            this.resource = new Resource.ProcedureResource( namespace, procedureName );
            break;
        default:
        }
        return this;
    }

    ResourcePrivilege build() throws InvalidArgumentsException
    {
        return new ResourcePrivilege( action, resource );
    }
}
