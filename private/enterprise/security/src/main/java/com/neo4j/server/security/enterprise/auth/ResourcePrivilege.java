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
        resource.assertValidCombination( action );
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
}
