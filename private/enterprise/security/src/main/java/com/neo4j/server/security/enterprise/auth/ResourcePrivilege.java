/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

public class ResourcePrivilege
{
    public static final String FULL_SCOPE = "*";

    private final Action action;
    private final Resource resource;
    private final String scope;

    public ResourcePrivilege( Action action, Resource resource )
    {
        this( action, resource, FULL_SCOPE );
    }

    public ResourcePrivilege( Action action, Resource resource, String scope )
    {
        this.action = action;
        this.resource = resource;
        this.scope = scope;
    }

    public ResourcePrivilege( String action, String resource, String scope )
    {
        this( Action.valueOf( action.toUpperCase() ), Resource.valueOf( resource.toUpperCase() ), scope );
    }

    public String getResource()
    {
        return resource.name().toLowerCase();
    }

    public String getScope()
    {
        return scope;
    }

    public String getAction()
    {
        return action.name().toLowerCase();
    }

    @Override
    public String toString()
    {
        return String.format( "(%s, %s, %s)", getAction(), getResource(), getScope() );
    }

    @Override
    public int hashCode()
    {
        int hash = 7 * action.hashCode();
        hash += 7 * resource.hashCode();
        hash += 7 * scope.hashCode();
        return hash;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof ResourcePrivilege )
        {
            ResourcePrivilege other = (ResourcePrivilege) obj;
            return other.action.equals( this.action ) && other.resource.equals( this.resource ) && other.scope.equals( this.scope );
        }
        return false;
    }

    // Actions that can be assigned to a privilege for a role
    public enum Action
    {
        READ,
        WRITE,
        EXECUTE;
    }

    // Type of resource a privilege applies to
    public enum Resource
    {
        GRAPH,
        SCHEMA,
        TOKEN,
        PROCEDURE
    }
}
