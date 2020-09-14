/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.internal.kernel.api.security.Segment;

public class RelTypeSegment implements Segment
{
    private final String relType;

    public RelTypeSegment( String relType )
    {
        this.relType = relType;
    }

    public String getRelType()
    {
        return relType;
    }

    @Override
    public int hashCode()
    {
        return relType.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }

        if ( obj instanceof RelTypeSegment )
        {
            RelTypeSegment other = (RelTypeSegment) obj;
            if ( this.relType == null )
            {
                return other.relType == null;
            }
            else
            {
                return this.relType.equals( other.getRelType() );
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format( "RELATIONSHIP %s", relType == null ? "*" : relType );
    }

    @Override
    public boolean satisfies( Segment segment )
    {
        if ( segment instanceof RelTypeSegment )
        {
            var other = (RelTypeSegment) segment;
            return relType == null || relType.equals( other.relType );
        }
        return false;
    }

    public static final RelTypeSegment ALL = new RelTypeSegment( null );
}
