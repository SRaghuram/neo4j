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
    private final Segment segment;
    private final String dbName;
    private final boolean allDatabases;
    private final boolean allowed;

    public ResourcePrivilege( Action action, Resource resource, Segment segment, Boolean allowed ) throws InvalidArgumentsException
    {
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = "";
        this.allowed = allowed;
        this.allDatabases = true;
        resource.assertValidCombination( action );
    }

    public ResourcePrivilege( Action action, Resource resource, Segment segment, Boolean allowed, String dbName ) throws InvalidArgumentsException
    {
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = dbName;
        this.allowed = allowed;
        this.allDatabases = false;
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

    public Segment getSegment()
    {
        return segment;
    }

    public String getDbName()
    {
        return this.dbName;
    }

    public boolean isAllowed()
    {
        return allowed;
    }

    public boolean isAllDatabases()
    {
        return allDatabases;
    }

    @Override
    public String toString()
    {
        String grant;
        if ( allowed )
        {
            grant = "GRANT";
        }
        else
        {
            grant = "DENY";
        }
        return String.format( "(%s, %s, %s, %s)", grant, getAction(), getResource(), getSegment() );
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
            return other.action.equals( this.action )
                    && other.resource.equals( this.resource )
                    && other.segment.equals( this.segment )
                    && other.dbName.equals( this.dbName )
                    && other.allowed == this.allowed
                    && other.allDatabases == this.allDatabases;
        }
        return false;
    }

    public enum Action
    {
        /** MATCH element and read labels */
        FIND,

        /** Read properties of element */
        READ,

        /** Create, update and delete elements and properties */
        WRITE,

        /** Execute procedure/view with elevated access */
        EXECUTE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }
}
