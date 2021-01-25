/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import static java.lang.String.format;

public class ReplicatedString implements ReplicatedContent
{
    private final String value;

    public ReplicatedString( String data )
    {
        this.value = data;
    }

    public static ReplicatedString valueOf( String value )
    {
        return new ReplicatedString( value );
    }

    public String get()
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

        ReplicatedString that = (ReplicatedString) o;
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
        return format( "ReplicatedString{data=%s}", value );
    }

    public String value()
    {
        return value;
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler )
    {
        throw new UnsupportedOperationException( "No handler for this " + this.getClass() );
    }
}
