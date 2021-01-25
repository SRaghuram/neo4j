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

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;

public class Election
{
    private Election()
    {
    }

    static boolean startRealElection( ReadableRaftState state, OutcomeBuilder outcomeBuilder, Log log, long term ) throws IOException
    {
        var currentMembers = state.votingMembers();
        if ( currentMembers == null || !currentMembers.contains( state.myself() ) )
        {
            log.info( "Election attempted but not started, current members are %s, I am %s",
                    currentMembers, state.myself() );
            return false;
        }

        term++;
        outcomeBuilder.setTerm( term );

        var voteForMe =
                new RaftMessages.Vote.Request( state.myself(), term, state.myself(), state.entryLog()
                        .appendIndex(), state.entryLog().readEntryTerm( state.entryLog().appendIndex() ) );

        currentMembers.stream().filter( member -> !member.equals( state.myself() ) ).forEach( member ->
                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( member, voteForMe ) )
        );

        outcomeBuilder.setVotedFor( state.myself() )
                .renewElectionTimer( ACTIVE_ELECTION );
        log.info( "Election started with vote request: %s and members: %s", voteForMe, currentMembers );
        return true;
    }

    static boolean startPreElection( ReadableRaftState state, OutcomeBuilder outcomeBuilder, Log log ) throws IOException
    {
        var currentMembers = state.votingMembers();
        if ( currentMembers == null || !currentMembers.contains( state.myself() ) )
        {
            log.info( "Pre-election attempted but not started, current members are %s, I am %s",
                    currentMembers, state.myself() );
            return false;
        }

        var preVoteForMe =
                new RaftMessages.PreVote.Request( state.myself(), state.term(), state.myself(), state.entryLog()
                        .appendIndex(), state.entryLog().readEntryTerm( state.entryLog().appendIndex() ) );

        currentMembers.stream().filter( member -> !member.equals( state.myself() ) ).forEach( member ->
                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( member, preVoteForMe ) )
        );
        outcomeBuilder.renewElectionTimer( ACTIVE_ELECTION );

        log.info( "Pre-election started with: %s and members: %s", preVoteForMe, currentMembers );
        return true;
    }
}
