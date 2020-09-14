/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.internal.kernel.api.security.Segment;

public class LabelSegment implements Segment
{
    private final String label;

    public LabelSegment( String label )
    {
        this.label = label;
    }

    public String getLabel()
    {
        return label;
    }

    @Override
    public int hashCode()
    {
        return label.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }

        if ( obj instanceof LabelSegment )
        {
            LabelSegment other = (LabelSegment) obj;
            if ( this.label == null )
            {
                return other.label == null;
            }
            else
            {
                return this.label.equals( other.getLabel() );
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format( "NODE %s", label == null ? "*" : label );
    }

    @Override
    public boolean satisfies( Segment segment )
    {
        if ( segment instanceof LabelSegment )
        {
            var other = (LabelSegment) segment;
            return label == null || label.equals( other.label );
        }
        return false;
    }

    public static final LabelSegment ALL = new LabelSegment( null );
}
