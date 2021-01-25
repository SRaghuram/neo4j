/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;

import java.io.IOException;

import org.neo4j.logging.Log;

class Heart
{
    private Heart()
    {
    }

    static void beat( ReadableRaftState state, OutcomeBuilder outcomeBuilder, RaftMessages.Heartbeat request, Log log )
            throws IOException
    {
        if ( request.leaderTerm() < state.term() )
        {
            return;
        }

        outcomeBuilder.setPreElection( false )
                .setTerm( request.leaderTerm() )
                .setLeader( request.from() )
                .setLeaderCommit( request.commitIndex() )
                .addOutgoingMessage( new RaftMessages.Directed( request.from(), new RaftMessages.HeartbeatResponse( state.myself() ) ) );

        if ( !Follower.logHistoryMatches( state, request.commitIndex(), request.commitIndexTerm() ) )
        {
            return;
        }

        Follower.commitToLogOnUpdate( state, request.commitIndex(), request.commitIndex(), outcomeBuilder );
    }
}
