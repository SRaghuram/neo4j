/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.MemberId;

import java.io.Serializable;
import java.util.Objects;

public class LeaderInfo implements Serializable
{
    private static final long serialVersionUID = 7983780359510842910L;

    public static final LeaderInfo INITIAL = new LeaderInfo( null, -1 );

    private final MemberId memberId;
    private final long term;
    private final boolean isSteppingDown;

    public LeaderInfo( MemberId memberId, long term )
    {
        this( memberId, term, false );
    }

    private LeaderInfo( MemberId memberId, long term, boolean isSteppingDown )
    {
        this.memberId = memberId;
        this.term = term;
        this.isSteppingDown = isSteppingDown;
    }

    /**
     * Produces a new LeaderInfo object for a step down event, setting memberId to null but maintaining the current term.
     */
    public LeaderInfo stepDown()
    {
        return new LeaderInfo( null, this.term, true );
    }

    public boolean isSteppingDown()
    {
        return isSteppingDown;
    }

    public MemberId memberId()
    {
        return memberId;
    }

    public long term()
    {
        return term;
    }

    @Override
    public String toString()
    {
        return "LeaderInfo{" + "memberId=" + memberId + ", term=" + term + '}';
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
        LeaderInfo that = (LeaderInfo) o;
        return term == that.term && isSteppingDown == that.isSteppingDown && Objects.equals( memberId, that.memberId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( memberId, term, isSteppingDown );
    }
}
