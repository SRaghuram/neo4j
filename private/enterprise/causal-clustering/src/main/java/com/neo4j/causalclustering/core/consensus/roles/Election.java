/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.Set;

import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;

public class Election
{
    private Election()
    {
    }

    static boolean startRealElection( ReadableRaftState ctx, OutcomeBuilder outcomeBuilder, Log log, long term ) throws IOException
    {
        Set<MemberId> currentMembers = ctx.votingMembers();
        if ( currentMembers == null || !currentMembers.contains( ctx.myself() ) )
        {
            log.info( "Election attempted but not started, current members are %s, I am %s",
                    currentMembers, ctx.myself() );
            return false;
        }

        term++;
        outcomeBuilder.setTerm( term );

        RaftMessages.Vote.Request voteForMe =
                new RaftMessages.Vote.Request( ctx.myself(), term, ctx.myself(), ctx.entryLog()
                        .appendIndex(), ctx.entryLog().readEntryTerm( ctx.entryLog().appendIndex() ) );

        currentMembers.stream().filter( member -> !member.equals( ctx.myself() ) ).forEach( member ->
                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( member, voteForMe ) )
        );

        outcomeBuilder.setVotedFor( ctx.myself() )
                .renewElectionTimer( ACTIVE_ELECTION );
        log.info( "Election started with vote request: %s and members: %s", voteForMe, currentMembers );
        return true;
    }

    static boolean startPreElection( ReadableRaftState ctx, OutcomeBuilder outcomeBuilder, Log log ) throws IOException
    {
        Set<MemberId> currentMembers = ctx.votingMembers();
        if ( currentMembers == null || !currentMembers.contains( ctx.myself() ) )
        {
            log.info( "Pre-election attempted but not started, current members are %s, I am %s",
                    currentMembers, ctx.myself() );
            return false;
        }

        RaftMessages.PreVote.Request preVoteForMe =
                new RaftMessages.PreVote.Request( ctx.myself(), ctx.term(), ctx.myself(), ctx.entryLog()
                        .appendIndex(), ctx.entryLog().readEntryTerm( ctx.entryLog().appendIndex() ) );

        currentMembers.stream().filter( member -> !member.equals( ctx.myself() ) ).forEach( member ->
                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( member, preVoteForMe ) )
        );
        outcomeBuilder.renewElectionTimer( ACTIVE_ELECTION );

        log.info( "Pre-election started with: %s and members: %s", preVoteForMe, currentMembers );
        return true;
    }
}
