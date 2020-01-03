/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

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
        return relType == null ? "All relationships" : "relType: '" + relType + "'";
    }

    public static final RelTypeSegment ALL = new RelTypeSegment( null );
}
