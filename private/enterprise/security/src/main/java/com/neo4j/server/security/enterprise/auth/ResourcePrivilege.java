/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;

public class ResourcePrivilege
{
    private final GrantOrDeny privilegeType;
    private final PrivilegeAction action;
    private final Resource resource;
    private final Segment segment;
    private final String dbName;
    private final boolean allDatabases, defaultDatabase;

    public ResourcePrivilege( GrantOrDeny privilegeType, PrivilegeAction action, Resource resource, Segment segment, SpecialDatabase specialDatabase )
            throws InvalidArgumentsException
    {
        this.privilegeType = privilegeType;
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = "";
        if ( specialDatabase.equals( SpecialDatabase.ALL ) )
        {
            this.allDatabases = true;
            this.defaultDatabase = false;
        }
        else
        {
            this.allDatabases = false;
            this.defaultDatabase = true;
        }
        resource.assertValidCombination( action );
    }

    public ResourcePrivilege( GrantOrDeny privilegeType, PrivilegeAction action, Resource resource, Segment segment, String dbName )
            throws InvalidArgumentsException
    {
        this.privilegeType = privilegeType;
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = dbName;
        this.allDatabases = false;
        this.defaultDatabase = false;
        resource.assertValidCombination( action );
    }

    boolean appliesTo( String database )
    {
        if ( database.equals( SYSTEM_DATABASE_NAME ) )
        {
            if ( ADMIN.satisfies( action ) )
            {
                return true;
            }
        }
        return allDatabases || database.equals( dbName );
    }

    boolean appliesToDefault()
    {
        return defaultDatabase;
    }

    GrantOrDeny getPrivilegeType()
    {
        return privilegeType;
    }

    Resource getResource()
    {
        return resource;
    }

    PrivilegeAction getAction()
    {
        return action;
    }

    Segment getSegment()
    {
        return segment;
    }

    String getDbName()
    {
        return this.dbName;
    }

    boolean appliesToAll()
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

    public enum SpecialDatabase
    {
        ALL,
        DEFAULT
    }
}
