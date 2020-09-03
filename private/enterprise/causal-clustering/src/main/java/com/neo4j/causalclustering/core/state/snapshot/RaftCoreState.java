/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;

import java.io.IOException;

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

    public static class Marshal extends SafeStateMarshal<RaftCoreState>
    {
        private static final MembershipEntry.Marshal MEMBERSHIP_MARSHAL = new MembershipEntry.Marshal();

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
