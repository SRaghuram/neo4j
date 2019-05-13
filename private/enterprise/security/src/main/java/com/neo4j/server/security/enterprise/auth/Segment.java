/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

public class Segment
{
    private final String label;

    public Segment( String label )
    {
        this.label = label;
    }

    public String getLabel()
    {
        return label;
    }

    public static Segment ALL = new Segment( null );

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

        if ( obj instanceof Segment )
        {
            Segment other = (Segment) obj;
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
        return label == null ? "All labels" : "label: '" + label + "'";
    }
}
