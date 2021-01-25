/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import static java.lang.String.format;

/**
 * Consistent leader state at a point in time.
 */
public class LeaderContext
{
    public final long term;
    public final long commitIndex;

    public LeaderContext( long term, long commitIndex )
    {
        this.term = term;
        this.commitIndex = commitIndex;
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

        LeaderContext that = (LeaderContext) o;

        if ( term != that.term )
        {
            return false;
        }
        return commitIndex == that.commitIndex;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return format( "LeaderContext{term=%d, commitIndex=%d}", term, commitIndex );
    }
}
