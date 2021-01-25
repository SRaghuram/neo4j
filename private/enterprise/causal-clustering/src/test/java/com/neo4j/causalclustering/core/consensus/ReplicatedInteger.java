/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.util.Objects;

import static java.lang.String.format;

public class ReplicatedInteger implements ReplicatedContent
{
    private final Integer value;

    private ReplicatedInteger( Integer data )
    {
        Objects.requireNonNull( data );
        this.value = data;
    }

    public static ReplicatedInteger valueOf( Integer value )
    {
        return new ReplicatedInteger( value );
    }

    public int get()
    {
        return value;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ReplicatedInteger that = (ReplicatedInteger) o;
        return value.equals( that.value );
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public String toString()
    {
        return format( "Integer(%d)", value );
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler )
    {
        throw new UnsupportedOperationException( "No handler for this " + this.getClass() );
    }
}
