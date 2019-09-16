/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public class ResourcePrivilege
{
    private final GrantOrDeny privilegeType;
    private final Action action;
    private final Resource resource;
    private final Segment segment;
    private final String dbName;
    private final boolean allDatabases;

    public ResourcePrivilege( GrantOrDeny privilegeType, Action action, Resource resource, Segment segment ) throws InvalidArgumentsException
    {
        this.privilegeType = privilegeType;
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = "";
        this.allDatabases = true;
        resource.assertValidCombination( action );
    }

    public ResourcePrivilege( GrantOrDeny privilegeType, Action action, Resource resource, Segment segment, String dbName ) throws InvalidArgumentsException
    {
        this.privilegeType = privilegeType;
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = dbName;
        this.allDatabases = false;
        resource.assertValidCombination( action );
    }

    public GrantOrDeny getPrivilegeType()
    {
        return privilegeType;
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
        return String.format( "(%s, %s, %s, %s)", privilegeType.prefix, getAction(), getResource(), getSegment() );
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
                    && other.privilegeType == this.privilegeType
                    && other.allDatabases == this.allDatabases;
        }
        return false;
    }

    public enum GrantOrDeny
    {
        GRANT( "GRANTED" ),
        DENY( "DENIED" );

        public final String name;
        public final String prefix;
        public final String relType;

        GrantOrDeny( String relType )
        {
            this.name = super.toString().toLowerCase();
            this.prefix = super.toString().toUpperCase();
            this.relType = relType;
        }

        public boolean isGrant()
        {
            return this == GRANT;
        }

        public boolean isDeny()
        {
            return this == DENY;
        }

        public static GrantOrDeny fromRelType( String relType )
        {
            for ( GrantOrDeny grantOrDeny : GrantOrDeny.values() )
            {
                if ( grantOrDeny.relType.equals( relType ) )
                {
                    return grantOrDeny;
                }
            }
            throw new IllegalArgumentException( "Unknown privilege type: " + relType );
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public enum Action
    {
        /** ACCESS database */
        ACCESS,

        /** START database */
        START,

        /** STOP database */
        STOP,

        /** MATCH element and read labels */
        TRAVERSE,

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
