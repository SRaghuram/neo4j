/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContext;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.configuration.ServerGroupName;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;
import static com.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static java.lang.Long.max;
import static java.lang.Long.min;

class Follower implements RaftMessageHandler
{
    static boolean logHistoryMatches( ReadableRaftState state, long leaderSegmentPrevIndex, long leaderSegmentPrevTerm ) throws IOException
    {
        // NOTE: A prevLogIndex before or at our log's prevIndex means that we
        //       already have all history (in a compacted form), so we report that history matches

        // NOTE: The entry term for a non existing log index is defined as -1,
        //       so the history for a non existing log entry never matches.

        long localLogPrevIndex = state.entryLog().prevIndex();
        long localSegmentPrevTerm = state.entryLog().readEntryTerm( leaderSegmentPrevIndex );

        return leaderSegmentPrevIndex > -1 && (leaderSegmentPrevIndex <= localLogPrevIndex || localSegmentPrevTerm == leaderSegmentPrevTerm);
    }

    static void commitToLogOnUpdate( ReadableRaftState state, long indexOfLastNewEntry, long leaderCommit, OutcomeBuilder outcomeBuilder )
    {
        long newCommitIndex = min( leaderCommit, indexOfLastNewEntry );

        if ( newCommitIndex > state.commitIndex() )
        {
            outcomeBuilder.setCommitIndex( newCommitIndex );
        }
    }

    private static void handleLeaderLogCompaction( ReadableRaftState state, OutcomeBuilder outcomeBuilder, RaftMessages.LogCompactionInfo compactionInfo )
    {
        if ( compactionInfo.leaderTerm() < state.term() )
        {
            return;
        }

        long localAppendIndex = state.entryLog().appendIndex();
        long leaderPrevIndex = compactionInfo.prevIndex();

        if ( localAppendIndex <= -1 || leaderPrevIndex > localAppendIndex )
        {
            outcomeBuilder.markNeedForFreshSnapshot( leaderPrevIndex, localAppendIndex );
        }
    }

    private static void handleLeadershipTransfer( RaftMessageHandlingContext ctx, OutcomeBuilder outcomeBuilder,
            RaftMessages.LeadershipTransfer.Request request, Log log ) throws IOException
    {
        var state = ctx.state();
        var sameTerm = state.term() == request.term();
        var localAppendIndex = state.entryLog().appendIndex();
        var upToDate = localAppendIndex >= request.previousIndex();
        var myGroups = ctx.serverGroups();

        var doesNotRefuseToBeLeader = !ctx.refusesToBeLeader();
        var satisfiesRequestPriorities = noRequestedPriority( request ) || iAmInPriority( myGroups, request );

        if ( doesNotRefuseToBeLeader && sameTerm && upToDate && satisfiesRequestPriorities )
        {
            if ( Election.startRealElection( state, outcomeBuilder, log, state.term() ) )
            {
                outcomeBuilder.setRole( CANDIDATE );
                log.info( "Moving to CANDIDATE state after receiving %s", request );
            }
        }
        else
        {
            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( request.from(),
                            new RaftMessages.LeadershipTransfer.Rejection( state.myself(), localAppendIndex, state.term() ) ) );
        }
    }

    private static boolean noRequestedPriority( RaftMessages.LeadershipTransfer.Request request )
    {
        return request.groups().isEmpty();
    }

    private static boolean iAmInPriority( Set<ServerGroupName> myGroups, RaftMessages.LeadershipTransfer.Request request )
    {
        for ( var priorityGroup : request.groups() )
        {
            if ( myGroups.contains( priorityGroup ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Outcome handle( RaftMessages.RaftMessage message, RaftMessageHandlingContext ctx, Log log ) throws IOException
    {
        return message.dispatch( visitor( ctx, log ) ).build();
    }

    private static class Handler implements RaftMessages.Handler<OutcomeBuilder,IOException>
    {
        protected final RaftMessageHandlingContext ctx;
        protected final ReadableRaftState state;
        protected final Log log;
        protected final OutcomeBuilder outcomeBuilder;
        private final PreVoteRequestHandler preVoteRequestHandler;
        private final PreVoteResponseHandler preVoteResponseHandler;

        Handler( PreVoteRequestHandler preVoteRequestHandler, PreVoteResponseHandler preVoteResponseHandler, RaftMessageHandlingContext ctx, Log log )
        {
            this.ctx = ctx;
            this.state = ctx.state();
            this.log = log;
            this.outcomeBuilder = OutcomeBuilder.builder( FOLLOWER, ctx.state() );
            this.preVoteRequestHandler = preVoteRequestHandler;
            this.preVoteResponseHandler = preVoteResponseHandler;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Heartbeat heartbeat ) throws IOException
        {
            Heart.beat( state, outcomeBuilder, heartbeat, log );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Request request ) throws IOException
        {
            Appending.handleAppendEntriesRequest( state, outcomeBuilder, request );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Request request ) throws IOException
        {
            var term = max( request.term(), state.term() );
            var votedFor = state.votedFor();
            if ( term > state.term() )
            {
                outcomeBuilder.setTerm( term );
                votedFor = null;
            }
            Voting.handleVoteVerdict( state, outcomeBuilder, term, request, log, votedFor );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            handleLeaderLogCompaction( state, outcomeBuilder, logCompactionInfo );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Response response )
        {
            log.info( "Late vote response: %s", response );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Request request ) throws IOException
        {
            return preVoteRequestHandler.handle( request, outcomeBuilder, state, log );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Response response ) throws IOException
        {
            return preVoteResponseHandler.handle( response, outcomeBuilder, state, log );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PruneRequest pruneRequest )
        {
            Pruning.handlePruneRequest( outcomeBuilder, pruneRequest );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Response response )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            return handleElectionTimeout( outcomeBuilder, ctx, log );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.NewEntry.Request request )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest ) throws IOException
        {
            handleLeadershipTransfer( ctx, outcomeBuilder, leadershipTransferRequest, log );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal ) throws IOException
        {
            return handle( new RaftMessages.LeadershipTransfer.Rejection( state.myself(), state.commitIndex(), state.term() ) );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection ) throws IOException
        {
            outcomeBuilder.addLeaderTransferRejection( leadershipTransferRejection );
            return outcomeBuilder;
        }
    }

    private interface PreVoteRequestHandler
    {
        OutcomeBuilder handle( RaftMessages.PreVote.Request request, OutcomeBuilder outcomeBuilder, ReadableRaftState state, Log log ) throws IOException;

    }
    private interface PreVoteResponseHandler
    {
        OutcomeBuilder handle( RaftMessages.PreVote.Response response, OutcomeBuilder outcomeBuilder, ReadableRaftState state, Log log ) throws IOException;
    }

    private static OutcomeBuilder handleElectionTimeout( OutcomeBuilder outcomeBuilder, RaftMessageHandlingContext ctx, Log log ) throws IOException
    {
        if ( ctx.supportPreVoting() && !ctx.refusesToBeLeader() )
        {
            log.info( "Election timeout triggered" );
            if ( Election.startPreElection( ctx.state(), outcomeBuilder, log ) )
            {
                outcomeBuilder.setPreElection( true );
            }
        }
        else if ( ctx.supportPreVoting() && ctx.refusesToBeLeader() )
        {
            log.info( "Election timeout triggered but refusing to be leader" );
            Set<MemberId> memberIds = ctx.state().votingMembers();
            if ( memberIds != null && memberIds.contains( ctx.state().myself() ) )
            {
                outcomeBuilder.setPreElection( true );
            }
        }
        else if ( !ctx.supportPreVoting() && ctx.refusesToBeLeader() )
        {
            log.info( "Election timeout triggered but refusing to be leader" );
        }
        else
        {
            log.info( "Election timeout triggered" );
            if ( Election.startRealElection( ctx.state(), outcomeBuilder, log, ctx.state().term() ) )
            {
                outcomeBuilder.setRole( CANDIDATE );
                log.info( "Moving to CANDIDATE state after successfully starting election" );
            }
        }

        return outcomeBuilder;
    }

    private static class PreVoteRequestVotingHandler implements PreVoteRequestHandler
    {
        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Request request, OutcomeBuilder outcome, ReadableRaftState state, Log log ) throws IOException
        {
            var term = max( state.term(), request.term() );
            outcome.setTerm( term );
            Voting.handlePreVoteVerdict( state, outcome, request, log, term );
            return outcome;
        }

        private static final PreVoteRequestHandler INSTANCE = new PreVoteRequestVotingHandler();
    }

    private static class PreVoteRequestDecliningHandler implements PreVoteRequestHandler
    {
        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Request request, OutcomeBuilder outcomeBuilder, ReadableRaftState state, Log log ) throws IOException
        {
            var term = max( request.term(), state.term() );
            if ( term > state.term() )
            {
                outcomeBuilder.setTerm( term );
            }
            Voting.declinePreVoteRequest( state, outcomeBuilder, request, term );
            return outcomeBuilder;
        }

        private static final PreVoteRequestHandler INSTANCE = new PreVoteRequestDecliningHandler();
    }

    private static class PreVoteRequestNoOpHandler implements PreVoteRequestHandler
    {
        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Request request, OutcomeBuilder outcomeBuilder, ReadableRaftState state, Log log )
        {
            return outcomeBuilder;
        }

        private static final PreVoteRequestHandler INSTANCE = new PreVoteRequestNoOpHandler();
    }

    private static class PreVoteResponseSolicitingHandler implements PreVoteResponseHandler
    {
        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Response res, OutcomeBuilder outcomeBuilder, ReadableRaftState state, Log log ) throws IOException
        {
            long term = max( res.term(), state.term() );
            if ( term > state.term() )
            {
                outcomeBuilder.setTerm( res.term() )
                        .setPreElection( false );
                log.info( "Aborting pre-election after receiving pre-vote response from %s at term %d (I am at %d)", res.from(), res.term(), state.term() );
                return outcomeBuilder;
            }
            else if ( res.term() < state.term() || !res.voteGranted() )
            {
                return outcomeBuilder;
            }

            var preVotesForMe = new HashSet<>( state.preVotesForMe() );

            if ( !res.from().equals( state.myself() ) )
            {
                preVotesForMe.add( res.from() );
                outcomeBuilder.setPreVotesForMe( preVotesForMe );
            }

            if ( isQuorum( state.votingMembers(), preVotesForMe ) )
            {
                outcomeBuilder.renewElectionTimer( ACTIVE_ELECTION )
                        .setPreElection( false );
                if ( Election.startRealElection( state, outcomeBuilder, log, term ) )
                {
                    outcomeBuilder.setRole( CANDIDATE );
                    log.info( "Moving to CANDIDATE state after successful pre-election stage" );
                }
            }
            return outcomeBuilder;
        }
        private static final PreVoteResponseHandler INSTANCE = new PreVoteResponseSolicitingHandler();
    }

    private static class PreVoteResponseNoOpHandler implements PreVoteResponseHandler
    {
        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Response response, OutcomeBuilder outcomeBuilder, ReadableRaftState state, Log log )
        {
            return outcomeBuilder;
        }

        private static final PreVoteResponseHandler INSTANCE = new PreVoteResponseNoOpHandler();
    }

    private static Handler visitor( RaftMessageHandlingContext ctx, Log log )
    {
        final var state = ctx.state();
        final PreVoteRequestHandler preVoteRequestHandler;
        final PreVoteResponseHandler preVoteResponseHandler;

        if ( ctx.refusesToBeLeader() )
        {
            preVoteResponseHandler = PreVoteResponseNoOpHandler.INSTANCE;
            if ( ctx.supportPreVoting() )
            {
                preVoteRequestHandler = ( state.isPreElection() || !state.areTimersStarted() ) ?
                        PreVoteRequestVotingHandler.INSTANCE : PreVoteRequestDecliningHandler.INSTANCE;
            }
            else
            {
                preVoteRequestHandler = PreVoteRequestNoOpHandler.INSTANCE;
            }
        }
        else
        {
            if ( ctx.supportPreVoting() )
            {
                preVoteRequestHandler = (state.isPreElection() || !state.areTimersStarted()) ?
                        PreVoteRequestVotingHandler.INSTANCE : PreVoteRequestDecliningHandler.INSTANCE;
                preVoteResponseHandler = (state.isPreElection()) ?
                        PreVoteResponseSolicitingHandler.INSTANCE : PreVoteResponseNoOpHandler.INSTANCE;
            }
            else
            {
                preVoteRequestHandler = PreVoteRequestNoOpHandler.INSTANCE;
                preVoteResponseHandler = PreVoteResponseNoOpHandler.INSTANCE;
            }
        }
        return new Handler( preVoteRequestHandler, preVoteResponseHandler, ctx, log );
    }
}
