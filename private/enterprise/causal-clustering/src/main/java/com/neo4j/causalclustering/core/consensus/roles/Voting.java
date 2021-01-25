/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;

import org.neo4j.function.ThrowingBooleanSupplier;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;

public class Voting
{

    private Voting()
    {
    }

    static void handleVoteVerdict( ReadableRaftState state, OutcomeBuilder outcomeBuilder,
            long term, RaftMessages.Vote.Request voteRequest, Log log, RaftMemberId votedFor ) throws IOException
    {
        boolean votedForAnother = votedFor != null && !votedFor.equals( voteRequest.candidate() );
        boolean willVoteForCandidate = shouldVoteFor( state, voteRequest, votedForAnother, log, term );

        if ( willVoteForCandidate )
        {
            outcomeBuilder.setVotedFor( voteRequest.from() )
                    .renewElectionTimer( ACTIVE_ELECTION );
        }

        outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.Vote.Response(
                state.myself(), term,
                willVoteForCandidate ) ) );
    }

    static void handlePreVoteVerdict( ReadableRaftState state, OutcomeBuilder outcomeBuilder,
            RaftMessages.PreVote.Request voteRequest, Log log, long term ) throws IOException
    {
        ThrowingBooleanSupplier<IOException> willVoteForCandidate =
                () -> shouldPreVoteFor( state, voteRequest, log, term );
        respondToPreVoteRequest( state, outcomeBuilder, voteRequest, willVoteForCandidate, term );
    }

    static void declinePreVoteRequest( ReadableRaftState state, OutcomeBuilder outcomeBuilder,
            RaftMessages.PreVote.Request voteRequest, long term ) throws IOException
    {
        respondToPreVoteRequest( state, outcomeBuilder, voteRequest, () -> false, term );
    }

    private static void respondToPreVoteRequest( ReadableRaftState state, OutcomeBuilder outcomeBuilder, RaftMessages.PreVote.Request voteRequest,
            ThrowingBooleanSupplier<IOException> willVoteFor, long term ) throws IOException
    {
        outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.PreVote.Response(
                state.myself(), term, willVoteFor.getAsBoolean() ) ) );
    }

    private static boolean shouldVoteFor( ReadableRaftState state, RaftMessages.Vote.Request voteRequest,
            boolean committedToVotingForAnother, Log log, long term )
            throws IOException
    {
        var requestTerm = voteRequest.term();
        var candidate = voteRequest.candidate();
        var requestLastLogTerm = voteRequest.lastLogTerm();
        var requestLastLogIndex = voteRequest.lastLogIndex();
        return shouldAnyVoteFor( requestTerm, candidate, requestLastLogTerm, requestLastLogIndex, state, log, term, committedToVotingForAnother, false );
    }

    private static boolean shouldPreVoteFor( ReadableRaftState state, RaftMessages.PreVote.Request voteRequest, Log log, long term ) throws IOException
    {
        var requestTerm = voteRequest.term();
        var candidate = voteRequest.candidate();
        var requestLastLogTerm = voteRequest.lastLogTerm();
        var requestLastLogIndex = voteRequest.lastLogIndex();
        return shouldAnyVoteFor( requestTerm, candidate, requestLastLogTerm, requestLastLogIndex, state, log, term, false, true );
    }

    private static boolean shouldAnyVoteFor( long requestTerm, RaftMemberId candidate, long requestLastLogTerm, long requestLastLogIndex,
            ReadableRaftState state, Log log, long term, boolean committedToVotingForAnother, boolean isPreVote ) throws IOException
    {
        long contextLastAppended = state.entryLog().appendIndex();
        long contextLastLogTerm = state.entryLog().readEntryTerm( contextLastAppended );

        return shouldAnyVoteFor(
                candidate,
                term,
                requestTerm,
                contextLastLogTerm,
                requestLastLogTerm,
                contextLastAppended,
                requestLastLogIndex,
                committedToVotingForAnother,
                log, isPreVote );
    }

    public static boolean shouldAnyVoteFor( RaftMemberId candidate, long contextTerm, long requestTerm, long contextLastLogTerm, long requestLastLogTerm,
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
