/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;

import org.neo4j.function.ThrowingBooleanSupplier;
import org.neo4j.logging.Log;

public class Voting
{

    private Voting()
    {
    }

    static void handleVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.Vote.Request voteRequest, Log log ) throws IOException
    {
        if ( voteRequest.term() > state.term() )
        {
            outcome.setNextTerm( voteRequest.term() );
            outcome.setVotedFor( null );
        }

        boolean votedForAnother = outcome.getVotedFor() != null && !outcome.getVotedFor().equals( voteRequest.candidate() );
        boolean willVoteForCandidate = shouldVoteFor( state, outcome, voteRequest, votedForAnother, log, false );

        if ( willVoteForCandidate )
        {
            outcome.setVotedFor( voteRequest.from() );
            outcome.renewElectionTimeout();
        }

        outcome.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.Vote.Response(
                state.myself(), outcome.getTerm(),
                willVoteForCandidate ) ) );
    }

    static void handlePreVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.PreVote.Request voteRequest, Log log ) throws IOException
    {
        ThrowingBooleanSupplier<IOException> willVoteForCandidate =
                () -> shouldVoteFor( state, outcome, voteRequest, false, log, true );
        respondToPreVoteRequest( state, outcome, voteRequest, willVoteForCandidate );
    }

    static void declinePreVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.PreVote.Request voteRequest ) throws IOException
    {
        respondToPreVoteRequest( state, outcome, voteRequest, () -> false );
    }

    private static void respondToPreVoteRequest( ReadableRaftState state, Outcome outcome, RaftMessages.PreVote.Request voteRequest,
            ThrowingBooleanSupplier<IOException> willVoteFor ) throws IOException
    {
        if ( voteRequest.term() > state.term() )
        {
            outcome.setNextTerm( voteRequest.term() );
        }

        outcome.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.PreVote.Response(
                state.myself(), outcome.getTerm(), willVoteFor.getAsBoolean() ) ) );
    }

    private static boolean shouldVoteFor( ReadableRaftState state, Outcome outcome, RaftMessages.AnyVote.Request voteRequest,
            boolean committedToVotingForAnother, Log log, boolean isPreVote )
            throws IOException
    {
        long requestTerm = voteRequest.term();
        MemberId candidate = voteRequest.candidate();
        long requestLastLogTerm = voteRequest.lastLogTerm();
        long requestLastLogIndex = voteRequest.lastLogIndex();
        long contextTerm = outcome.getTerm();
        long contextLastAppended = state.entryLog().appendIndex();
        long contextLastLogTerm = state.entryLog().readEntryTerm( contextLastAppended );

        return shouldVoteFor(
                candidate,
                contextTerm,
                requestTerm,
                contextLastLogTerm,
                requestLastLogTerm,
                contextLastAppended,
                requestLastLogIndex,
                committedToVotingForAnother,
                log, isPreVote );
    }

    public static boolean shouldVoteFor( MemberId candidate, long contextTerm, long requestTerm, long contextLastLogTerm, long requestLastLogTerm,
            long contextLastAppended, long requestLastLogIndex, boolean committedToVotingForAnother, Log log, boolean isPreVote )
    {
        String voteType = isPreVote ? "pre-vote" : "vote";
        boolean requestFromPreviousTerm = requestTerm < contextTerm;
        boolean requestLogEndsAtHigherTerm = requestLastLogTerm > contextLastLogTerm;
        boolean logsEndAtSameTerm = requestLastLogTerm == contextLastLogTerm;
        boolean requestLogAtLeastAsLongAsMyLog = requestLastLogIndex >= contextLastAppended;

        boolean requesterLogUpToDate = requestLogEndsAtHigherTerm ||
                (logsEndAtSameTerm && requestLogAtLeastAsLongAsMyLog);

        boolean votedForOtherInSameTerm = requestTerm == contextTerm && committedToVotingForAnother;

        boolean shouldVoteFor = requesterLogUpToDate && !votedForOtherInSameTerm && !requestFromPreviousTerm;

        log.info( "Received raft %s request from %s. Should vote for candidate: %s", voteType, candidate, shouldVoteFor );

        log.debug( "Should vote for raft candidate: %s. Reasons for decision:%n" +
                        "requester log up to date: %s " +
                        "(request last log term: %s, context last log term: %s, request last log index: %s, context last append: %s),%n" +
                        "requester from an earlier term: %s " +
                        "(request term: %s, context term: %s),%n" +
                        "voted for other in same term: %s",
                shouldVoteFor,
                requesterLogUpToDate, requestLastLogTerm, contextLastLogTerm, requestLastLogIndex, contextLastAppended,
                requestFromPreviousTerm, requestTerm, contextTerm, votedForOtherInSameTerm );

        return shouldVoteFor;
    }
}
