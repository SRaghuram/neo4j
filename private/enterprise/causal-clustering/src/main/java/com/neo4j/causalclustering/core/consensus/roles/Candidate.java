/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContext;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.FAILURE_DETECTION;
import static com.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static java.lang.Long.max;

class Candidate implements RaftMessageHandler
{
    @Override
    public Outcome handle( RaftMessages.RaftMessage message, RaftMessageHandlingContext ctx, Log log ) throws IOException
    {
        return message.dispatch( new Handler( ctx, log ) ).build();
    }

    private static class Handler implements RaftMessages.Handler<OutcomeBuilder,IOException>
    {
        private final RaftMessageHandlingContext ctx;
        private final ReadableRaftState state;
        private final Log log;
        private final OutcomeBuilder outcomeBuilder;

        private Handler( RaftMessageHandlingContext ctx, Log log )
        {
            this.ctx = ctx;
            this.state = ctx.state();
            this.log = log;
            this.outcomeBuilder = OutcomeBuilder.builder( CANDIDATE, ctx.state() );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Heartbeat req ) throws IOException
        {
            if ( req.leaderTerm() < state.term() )
            {
                return outcomeBuilder;
            }

            outcomeBuilder.setRole( FOLLOWER );
            log.info( "Moving to FOLLOWER state after receiving heartbeat from %s at term %d (I am at %d)",
                    req.from(), req.leaderTerm(), state.term() );
            Heart.beat( state, outcomeBuilder, req, log );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Request req ) throws IOException
        {
            if ( req.leaderTerm() < state.term() )
            {
                RaftMessages.AppendEntries.Response appendResponse =
                        new RaftMessages.AppendEntries.Response( state.myself(), state.term(), false,
                                req.prevLogIndex(), state.entryLog().appendIndex() );

                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( req.from(), appendResponse ) );
                return outcomeBuilder;
            }

            outcomeBuilder.setRole( FOLLOWER );
            log.info( "Moving to FOLLOWER state after receiving append entries request from %s at term %d (I am at %d)n",
                    req.from(), req.leaderTerm(), state.term() );
            Appending.handleAppendEntriesRequest( state, outcomeBuilder, req );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Response res ) throws IOException
        {
            if ( res.term() > state.term() )
            {
                outcomeBuilder.setTerm( res.term() )
                        .setRole( FOLLOWER );
                log.info( "Moving to FOLLOWER state after receiving vote response from %s at term %d (I am at %d)",
                        res.from(), res.term(), state.term() );
                return outcomeBuilder;
            }
            else if ( res.term() < state.term() || !res.voteGranted() )
            {
                return outcomeBuilder;
            }

            var votesForMe = new HashSet<>( state.votesForMe() );
            if ( !res.from().equals( state.myself() ) )
            {
                votesForMe.add( res.from() );
                outcomeBuilder.setVotesForMe( votesForMe );
            }

            if ( isQuorum( state.votingMembers(), votesForMe ) )
            {
                outcomeBuilder.setLeader( state.myself() );
                Appending.appendNewEntry( state, outcomeBuilder, new NewLeaderBarrier() );
                Leader.sendHeartbeats( state, outcomeBuilder );

                outcomeBuilder.setLastLogIndexBeforeWeBecameLeader( state.entryLog().appendIndex() )
                        .electedLeader()
                        .renewElectionTimer( FAILURE_DETECTION )
                        .setRole( LEADER );
                log.info( "Moving to LEADER state at term %d (I am %s), voted for by %s", state.term(), state.myself(), votesForMe );
            }
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Request req ) throws IOException
        {
            long term = max( req.term(), state.term() );
            if ( term > state.term() )
            {
                outcomeBuilder.setTerm( term )
                        .setVotesForMe( Set.of() )
                        .setRole( FOLLOWER );
                log.info( "Moving to FOLLOWER state after receiving vote request from %s at term %d (I am at %d)",
                        req.from(), req.term(), state.term() );
                MemberId votedFor = null;
                Voting.handleVoteVerdict( state, outcomeBuilder, term, req, log, votedFor );
                return outcomeBuilder;
            }

            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( req.from(), new RaftMessages.Vote.Response( state.myself(), term, false ) ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            log.info( "Failed to get elected. Got votes from: %s", state.votesForMe() );
            if ( !Election.startRealElection( state, outcomeBuilder, log, state.term() ) )
            {
                log.info( "Moving to FOLLOWER state after failing to start election" );
                outcomeBuilder.setRole( FOLLOWER );
            }
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Request req ) throws IOException
        {
            if ( ctx.supportPreVoting() )
            {
                if ( req.term() > state.term() )
                {
                    outcomeBuilder.setVotesForMe( Set.of() )
                            .setRole( FOLLOWER );
                    log.info( "Moving to FOLLOWER state after receiving pre vote request from %s at term %d (I am at %d)",
                            req.from(), req.term(), state.term() );
                }
                var term = max( req.term(), state.term() );
                if ( term > state.term() )
                {
                    outcomeBuilder.setTerm( term );
                }
                Voting.handlePreVoteVerdict( state, outcomeBuilder, req, log, term );
            }
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Response response )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Response response )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return outcomeBuilder;
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
        public OutcomeBuilder handle( RaftMessages.PruneRequest pruneRequest )
        {
            Pruning.handlePruneRequest( outcomeBuilder, pruneRequest );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest )
        {
            var rejection = new RaftMessages.LeadershipTransfer.Rejection( state.myself(), state.commitIndex(), state.term() );
            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( leadershipTransferRequest.from(), rejection ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal )
        {
            return handle( new RaftMessages.LeadershipTransfer.Rejection( state.myself(), state.commitIndex(), state.term() ) );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
        {
            outcomeBuilder.addLeaderTransferRejection( leadershipTransferRejection );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.StatusResponse statusResponse ) throws IOException
        {
            outcomeBuilder.setStatusResponse( statusResponse );
            return outcomeBuilder;
        }
    }
}
