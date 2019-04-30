/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Collections;
import java.util.Set;

public class Segment
{
    private final Set<String> labels;

    public Segment( Set<String> labels )
    {
        this.labels = labels;
    }

    public Set<String> getLabels()
    {
        return labels;
    }

    public static Segment ALL = new Segment( Collections.emptySet() );

    @Override
    public int hashCode()
    {
        return labels.hashCode();
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
            return other.getLabels().equals( this.labels );
        }
        return false;
    }
}
