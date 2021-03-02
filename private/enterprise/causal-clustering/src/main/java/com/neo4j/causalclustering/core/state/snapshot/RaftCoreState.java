/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeStateMarshal;

public class RaftCoreState
{
    private MembershipEntry committed;

    public RaftCoreState( MembershipEntry committed )
    {
        this.committed = committed;
    }

    public MembershipEntry committed()
    {
        return committed;
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
        RaftCoreState that = (RaftCoreState) o;
        return Objects.equals( committed, that.committed );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( committed );
    }

    public static class Marshal extends SafeStateMarshal<RaftCoreState>
    {
        public static final Marshal INSTANCE = new Marshal();

        private static final MembershipEntry.Marshal MEMBERSHIP_MARSHAL = MembershipEntry.Marshal.INSTANCE;

        private Marshal()
        {
        }

        @Override
        public RaftCoreState startState()
        {
            return null;
        }

        @Override
        public long ordinal( RaftCoreState raftCoreState )
        {
            return 0;
        }

        @Override
        public void marshal( RaftCoreState raftCoreState, WritableChannel channel ) throws IOException
        {

            MEMBERSHIP_MARSHAL.marshal( raftCoreState.committed(), channel );
        }

        @Override
        protected RaftCoreState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            return new RaftCoreState( MEMBERSHIP_MARSHAL.unmarshal( channel ) );
        }
    }

    @Override
    public String toString()
    {
        return "RaftCoreState{" +
               "committed=" + committed +
               '}';
    }
}
