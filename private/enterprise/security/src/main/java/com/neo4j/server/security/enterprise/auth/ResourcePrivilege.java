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

    public ResourcePrivilege( Action action, Resource resource, Segment segment ) throws InvalidArgumentsException
    {
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = "";
        this.allDatabases = true;
        resource.assertValidCombination( action );
    }

    public ResourcePrivilege( Action action, Resource resource, Segment segment, String dbName ) throws InvalidArgumentsException
    {
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = dbName;
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

    public boolean isAllDatabases()
    {
        return allDatabases;
    }

    @Override
    public String toString()
    {
        return String.format( "(%s, %s, %s)", getAction(), getResource(), getSegment() );
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
