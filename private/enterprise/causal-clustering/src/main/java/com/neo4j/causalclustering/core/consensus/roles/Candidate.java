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
    public Outcome handle( RaftMessages.RaftMessage message, ReadableRaftState ctx, Log log ) throws IOException
    {
        return message.dispatch( new Handler( ctx, log ) ).build();
    }

    private static class Handler implements RaftMessages.Handler<OutcomeBuilder,IOException>
    {
        private final ReadableRaftState ctx;
        private final Log log;
        private final OutcomeBuilder outcomeBuilder;

        private Handler( ReadableRaftState ctx, Log log )
        {
            this.ctx = ctx;
            this.log = log;
            this.outcomeBuilder = OutcomeBuilder.builder( CANDIDATE, ctx );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Heartbeat req ) throws IOException
        {
            if ( req.leaderTerm() < ctx.term() )
            {
                return outcomeBuilder;
            }

            outcomeBuilder.setRole( FOLLOWER );
            log.info( "Moving to FOLLOWER state after receiving heartbeat from %s at term %d (I am at %d)",
                    req.from(), req.leaderTerm(), ctx.term() );
            Heart.beat( ctx, outcomeBuilder, req, log );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Request req ) throws IOException
        {
            if ( req.leaderTerm() < ctx.term() )
            {
                RaftMessages.AppendEntries.Response appendResponse =
                        new RaftMessages.AppendEntries.Response( ctx.myself(), ctx.term(), false,
                                req.prevLogIndex(), ctx.entryLog().appendIndex() );

                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( req.from(), appendResponse ) );
                return outcomeBuilder;
            }

            outcomeBuilder.setRole( FOLLOWER );
            log.info( "Moving to FOLLOWER state after receiving append entries request from %s at term %d (I am at %d)n",
                    req.from(), req.leaderTerm(), ctx.term() );
            Appending.handleAppendEntriesRequest( ctx, outcomeBuilder, req );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Response res ) throws IOException
        {
            if ( res.term() > ctx.term() )
            {
                outcomeBuilder.setTerm( res.term() )
                        .setRole( FOLLOWER );
                log.info( "Moving to FOLLOWER state after receiving vote response from %s at term %d (I am at %d)",
                        res.from(), res.term(), ctx.term() );
                return outcomeBuilder;
            }
            else if ( res.term() < ctx.term() || !res.voteGranted() )
            {
                return outcomeBuilder;
            }

            var votesForMe = new HashSet<>( ctx.votesForMe() );
            if ( !res.from().equals( ctx.myself() ) )
            {
                votesForMe.add( res.from() );
                outcomeBuilder.setVotesForMe( votesForMe );
            }

            if ( isQuorum( ctx.votingMembers(), votesForMe ) )
            {
                outcomeBuilder.setLeader( ctx.myself() );
                Appending.appendNewEntry( ctx, outcomeBuilder, new NewLeaderBarrier() );
                Leader.sendHeartbeats( ctx, outcomeBuilder );

                outcomeBuilder.setLastLogIndexBeforeWeBecameLeader( ctx.entryLog().appendIndex() )
                        .electedLeader()
                        .renewElectionTimer( FAILURE_DETECTION )
                        .setRole( LEADER );
                log.info( "Moving to LEADER state at term %d (I am %s), voted for by %s", ctx.term(), ctx.myself(), votesForMe );
            }
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Request req ) throws IOException
        {
            long term = max( req.term(), ctx.term() );
            if ( term > ctx.term() )
            {
                outcomeBuilder.setTerm( term )
                        .setVotesForMe( Set.of() )
                        .setRole( FOLLOWER );
                log.info( "Moving to FOLLOWER state after receiving vote request from %s at term %d (I am at %d)",
                        req.from(), req.term(), ctx.term() );
                MemberId votedFor = null;
                Voting.handleVoteVerdict( ctx, outcomeBuilder, term, req, log, votedFor );
                return outcomeBuilder;
            }

            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( req.from(), new RaftMessages.Vote.Response( ctx.myself(), term, false ) ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            log.info( "Failed to get elected. Got votes from: %s", ctx.votesForMe() );
            if ( !Election.startRealElection( ctx, outcomeBuilder, log, ctx.term() ) )
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
                if ( req.term() > ctx.term() )
                {
                    outcomeBuilder.setVotesForMe( Set.of() )
                            .setRole( FOLLOWER );
                    log.info( "Moving to FOLLOWER state after receiving pre vote request from %s at term %d (I am at %d)",
                            req.from(), req.term(), ctx.term() );
                }
                var term = max( req.term(), ctx.term() );
                if ( term > ctx.term() )
                {
                    outcomeBuilder.setTerm( term );
                }
                Voting.handlePreVoteVerdict( ctx, outcomeBuilder, req, log, term );
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
            var rejection = new RaftMessages.LeadershipTransfer.Rejection( ctx.myself(), ctx.commitIndex(), ctx.term() );
            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( leadershipTransferRequest.from(), rejection ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal )
        {
            return handle( new RaftMessages.LeadershipTransfer.Rejection( ctx.myself(), ctx.commitIndex(), ctx.term() ) );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
        {
            outcomeBuilder.addLeaderTransferRejection( leadershipTransferRejection );
            return outcomeBuilder;
        }
    }
}
