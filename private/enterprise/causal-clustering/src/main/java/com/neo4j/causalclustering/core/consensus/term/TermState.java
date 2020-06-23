/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.term;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;

public class TermState
{
    private volatile long term;

    public TermState()
    {
    }

    private TermState( long term )
    {
        this.term = term;
    }

    public long currentTerm()
    {
        return term;
    }

    /**
     * Updates the term to a new value. This value is generally expected, but not required, to be persisted. Consecutive
     * calls to this method should always have monotonically increasing arguments, thus maintaining the raft invariant
     * that the term is always non-decreasing. {@link IllegalArgumentException} can be thrown if an invalid value is
     * passed as argument.
     *
     * @param newTerm The new value.
     */
    public boolean update( long newTerm )
    {
        failIfInvalid( newTerm );
        boolean changed = term != newTerm;
        term = newTerm;
        return changed;
    }

    /**
     * This method implements the invariant of this class, that term never transitions to lower values. If
     * newTerm is lower than the term already stored in this class, it will throw an
     * {@link IllegalArgumentException}.
     */
    private void failIfInvalid( long newTerm )
    {
        if ( newTerm < term )
        {
            throw new IllegalArgumentException( "Cannot move to a lower term" );
        }
    }

    public static class Marshal extends SafeStateMarshal<TermState>
    {
        @Override
        public void marshal( TermState termState, WritableChannel channel ) throws IOException
        {
            channel.putLong( termState.currentTerm() );
        }

        @Override
        protected TermState unmarshal0( ReadableChannel channel ) throws IOException
        {
            return new TermState( channel.getLong() );
        }

        @Override
        public TermState startState()
        {
            return new TermState();
        }

        @Override
        public long ordinal( TermState state )
        {
            return state.currentTerm();
        }
    }

    @Override
    public String toString()
    {
        return "TermState{" +
               "term=" + term +
               '}';
    }
}
