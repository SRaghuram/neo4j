/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.util.Objects;

import static java.lang.String.format;

public class RaftLogEntry
{
    public static final RaftLogEntry[] empty = new RaftLogEntry[0];

    private final long term;
    private final ReplicatedContent content;

    public RaftLogEntry( long term, ReplicatedContent content )
    {
        Objects.requireNonNull( content );
        this.term = term;
        this.content = content;
    }

    public long term()
    {
        return this.term;
    }

    public ReplicatedContent content()
    {
        return this.content;
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

        RaftLogEntry that = (RaftLogEntry) o;

        return term == that.term && content.equals( that.content );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + content.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "{term=%d, content=%s}", term, content );
    }

}
