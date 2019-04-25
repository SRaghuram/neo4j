/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public class ResourcePrivilege
{
    private final Action action;
    private final Resource resource;

    public ResourcePrivilege( Action action, Resource resource ) throws InvalidArgumentsException
    {
        this.action = action;
        this.resource = resource;
        assertValidPrivilege();
    }

    public ResourcePrivilege( String action, String resource ) throws InvalidArgumentsException
    {
        Action asAction;
        Resource asResource;
        try
        {
            asAction = Action.valueOf( action.toUpperCase() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new InvalidArgumentsException( action + " is not a valid action" );
        }
        try
        {
            asResource = Resource.valueOf( resource.toUpperCase() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new InvalidArgumentsException( resource + " is not a valid resource" );
        }

        this.action = asAction;
        this.resource = asResource;
        assertValidPrivilege();
    }

    private void assertValidPrivilege() throws InvalidArgumentsException
    {
        switch ( action )
        {
        case READ:
            switch ( resource )
            {
            case GRAPH:
                break;
            case TOKEN:
            case SCHEMA:
            case SYSTEM:
            case PROCEDURE:
            default:
                throw new InvalidArgumentsException(
                        String.format( "Invalid combination of action (%s) and resource (%s)", action.toString(), resource.toString() ) );
            }
            break;
        case WRITE:
            switch ( resource )
            {
            case GRAPH:
            case TOKEN:
            case SCHEMA:
            case SYSTEM:
                break;
            case PROCEDURE:
            default:
                throw new InvalidArgumentsException(
                        String.format( "Invalid combination of action (%s) and resource (%s)", action.toString(), resource.toString() ) );
            }
            break;
        case EXECUTE:
            switch ( resource )
            {
            case PROCEDURE:
                break;
            case GRAPH:
            case TOKEN:
            case SCHEMA:
            case SYSTEM:
            default:
                throw new InvalidArgumentsException(
                        String.format( "Invalid combination of action (%s) and resource (%s)", action.toString(), resource.toString() ) );
            }
            break;
        default:
        }
    }

    public Resource getResource()
    {
        return resource;
    }

    public Action getAction()
    {
        return action;
    }

    @Override
    public String toString()
    {
        return String.format( "(%s, %s)", getAction(), getResource() );
    }

    @Override
    public int hashCode()
    {
        return action.hashCode() + 31 * resource.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof ResourcePrivilege )
        {
            ResourcePrivilege other = (ResourcePrivilege) obj;
            return other.action.equals( this.action ) && other.resource.equals( this.resource );
        }
        return false;
    }

    // Actions that can be assigned to a privilege for a role
    public enum Action
    {
        READ,
        WRITE,
        EXECUTE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

    // Type of resource a privilege applies to
    public enum Resource
    {
        GRAPH,
        SCHEMA,
        TOKEN,
        PROCEDURE,
        SYSTEM;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }
}
