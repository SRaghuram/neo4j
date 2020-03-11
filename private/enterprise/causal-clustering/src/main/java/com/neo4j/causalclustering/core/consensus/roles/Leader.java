/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.Followers;
import com.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum;
import com.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.neo4j.internal.helpers.collection.FilteringIterable;
import org.neo4j.logging.Log;

import static java.lang.Math.max;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

public class Leader implements RaftMessageHandler
{
    private static Iterable<MemberId> replicationTargets( final ReadableRaftState ctx )
    {
        return new FilteringIterable<>( ctx.replicationMembers(), member -> !member.equals( ctx.myself() ) );
    }

    static void sendHeartbeats( ReadableRaftState ctx, OutcomeBuilder outcomeBuilder ) throws IOException
    {
        long commitIndex = ctx.commitIndex();
        long commitIndexTerm = ctx.entryLog().readEntryTerm( commitIndex );
        RaftMessages.Heartbeat heartbeat = new RaftMessages.Heartbeat( ctx.myself(), ctx.term(), commitIndex, commitIndexTerm );
        for ( MemberId to : replicationTargets( ctx ) )
        {
            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( to, heartbeat ) );
        }
    }

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

        Handler( ReadableRaftState ctx, Log log )
        {
            this.ctx = ctx;
            this.log = log;
            this.outcomeBuilder = OutcomeBuilder.builder( Role.LEADER, ctx );
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Heartbeat heartbeat ) throws IOException
        {
            if ( heartbeat.leaderTerm() < ctx.term() )
            {
                return outcomeBuilder;
            }

            stepDownToFollower( outcomeBuilder, ctx );
            log.info( "Moving to FOLLOWER state after receiving heartbeat at term %d (my term is " + "%d) from %s",
                    heartbeat.leaderTerm(), ctx.term(), heartbeat.from() );
            Heart.beat( ctx, outcomeBuilder, heartbeat, log );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws IOException
        {
            sendHeartbeats( ctx, outcomeBuilder );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            outcomeBuilder.addHeartbeatResponse( heartbeatResponse.from() );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Timeout.Election election )
        {
            if ( !MajorityIncludingSelfQuorum.isQuorum( ctx.votingMembers().size(), ctx.heartbeatResponses().size() ) )
            {
                stepDownToFollower( outcomeBuilder, ctx );
                log.info( "Moving to FOLLOWER state after not receiving heartbeat responses in this election timeout " +
                          "period. Heartbeats received: %s", ctx.heartbeatResponses() );
            }

            outcomeBuilder.clearHeartbeatResponses();
            return outcomeBuilder;

        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Request req ) throws IOException
        {
            if ( req.leaderTerm() < ctx.term() )
            {
                RaftMessages.AppendEntries.Response appendResponse =
                        new RaftMessages.AppendEntries.Response( ctx.myself(), ctx.term(), false, -1,
                                ctx.entryLog().appendIndex() );

                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( req.from(), appendResponse ) );
                return outcomeBuilder;
            }
            else if ( req.leaderTerm() == ctx.term() )
            {
                throw new IllegalStateException( "Two leaders in the same term." );
            }
            else
            {
                // There is a new leader in a later term, we should revert to follower. (§5.1)
                stepDownToFollower( outcomeBuilder, ctx );
                log.info( "Moving to FOLLOWER state after receiving append request at term %d (my term is " +
                        "%d) from %s", req.leaderTerm(), ctx.term(), req.from() );
                Appending.handleAppendEntriesRequest( ctx, outcomeBuilder, req );
                return outcomeBuilder;
            }

        }

        @Override
        public OutcomeBuilder handle( RaftMessages.AppendEntries.Response response ) throws IOException
        {
            var followerStates = ctx.followerStates();
            if ( response.term() < ctx.term() )
            {
                /* Ignore responses from old terms! */
                return outcomeBuilder;
            }
            else if ( response.term() > ctx.term() )
            {
                outcomeBuilder.setTerm( response.term() );
                stepDownToFollower( outcomeBuilder, ctx );
                log.info( "Moving to FOLLOWER state after receiving append response at term %d (my term is " +
                          "%d) from %s", response.term(), ctx.term(), response.from() );
                followerStates = new FollowerStates<>();
                outcomeBuilder.replaceFollowerStates( followerStates );
                return outcomeBuilder;
            }

            FollowerState follower = ctx.followerStates().get( response.from() );

            if ( response.success() )
            {
                assert response.matchIndex() <= ctx.entryLog().appendIndex();

                boolean followerProgressed = response.matchIndex() > follower.getMatchIndex();

                followerStates = followerStates
                        .onSuccessResponse( response.from(), max( response.matchIndex(), follower.getMatchIndex() ) );
                outcomeBuilder.replaceFollowerStates( followerStates )
                        .addShipCommand( new ShipCommand.Match( response.matchIndex(), response.from() ) );

                /*
                 * Matches from older terms can in complicated leadership change / log truncation scenarios
                 * be overwritten, even if they were replicated to a majority of instances. Thus we must only
                 * consider matches from this leader's term when figuring out which have been safely replicated
                 * and are ready for commit.
                 * This is explained nicely in Figure 3.7 of the thesis
                 */
                boolean matchInCurrentTerm = ctx.entryLog().readEntryTerm( response.matchIndex() ) == ctx.term();

                /*
                 * The quorum situation may have changed only if the follower actually progressed.
                 */
                if ( followerProgressed && matchInCurrentTerm )
                {
                    // TODO: Test that mismatch between voting and participating members affects commit outcome

                    long quorumAppendIndex = Followers.quorumAppendIndex( ctx.votingMembers(), followerStates );
                    if ( quorumAppendIndex > ctx.commitIndex() )
                    {
                        outcomeBuilder.setLeaderCommit( quorumAppendIndex )
                                .setCommitIndex( quorumAppendIndex )
                                .addShipCommand( new ShipCommand.CommitUpdate() );
                    }
                }
            }
            else // Response indicated failure.
            {
                if ( response.appendIndex() > -1 && response.appendIndex() >= ctx.entryLog().prevIndex() )
                {
                    // Signal a mismatch to the log shipper, which will serve an earlier entry.
                    outcomeBuilder.addShipCommand( new ShipCommand.Mismatch( response.appendIndex(), response.from() ) );
                }
                else
                {
                    // There are no earlier entries, message the follower that we have compacted so that
                    // it can take appropriate action.
                    RaftMessages.LogCompactionInfo compactionInfo =
                            new RaftMessages.LogCompactionInfo( ctx.myself(), ctx.term(), ctx.entryLog().prevIndex() );
                    RaftMessages.Directed directedCompactionInfo =
                            new RaftMessages.Directed( response.from(), compactionInfo );

                    outcomeBuilder.addOutgoingMessage( directedCompactionInfo );
                }
            }
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Request req ) throws IOException
        {
            if ( req.term() > ctx.term() )
            {
                outcomeBuilder.setTerm( req.term() );
                stepDownToFollower( outcomeBuilder, ctx );
                log.info(
                        "Moving to FOLLOWER state after receiving vote request at term %d (my term is " + "%d) from %s",
                        req.term(), ctx.term(), req.from() );

                MemberId votedFor = null;
                Voting.handleVoteVerdict( ctx, outcomeBuilder, req.term(), req, log, votedFor );
                return outcomeBuilder;
            }

            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( req.from(),
                    new RaftMessages.Vote.Response( ctx.myself(), ctx.term(), false ) ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.NewEntry.Request req ) throws IOException
        {
            ReplicatedContent content = req.content();
            Appending.appendNewEntry( ctx, outcomeBuilder, content );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.NewEntry.BatchRequest req ) throws IOException
        {
            Collection<ReplicatedContent> contents = req.contents();
            Appending.appendNewEntries( ctx, outcomeBuilder, contents );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PruneRequest pruneRequest )
        {
            Pruning.handlePruneRequest( outcomeBuilder, pruneRequest );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.Vote.Response response )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Request req ) throws IOException
        {
            if ( ctx.supportPreVoting() )
            {
                if ( req.term() > ctx.term() )
                {
                    stepDownToFollower( outcomeBuilder, ctx );
                    log.info( "Moving to FOLLOWER state after receiving pre vote request from %s at term %d (I am at %d)",
                            req.from(), req.term(), ctx.term() );
                }
                var term = Long.max( req.term(), ctx.term() );
                if ( term > ctx.term() )
                {
                    outcomeBuilder.setTerm( term );
                }
                Voting.declinePreVoteRequest( ctx, outcomeBuilder, req, term );
            }
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.PreVote.Response response )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest ) throws IOException
        {
            if ( leadershipTransferRequest.term() > ctx.term() )
            {
                stepDownToFollower( outcomeBuilder, ctx );
                log.info( "Moving to FOLLOWER state after receiving leadership transfer request from %s at term %d (I am at %d)",
                          leadershipTransferRequest.from(), leadershipTransferRequest.term(), ctx.term() );
            }
            //TODO: Add my groups
            Set<String> myGroups = Set.of();
            var rejection = new RaftMessages.LeadershipTransfer.Rejection( leadershipTransferRequest.from(), ctx.commitIndex(), ctx.term(), myGroups );
            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( leadershipTransferRequest.from(), rejection ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal ) throws IOException
        {
            long commitIndex = ctx.commitIndex();
            long commitIndexTerm = ctx.entryLog().readEntryTerm( commitIndex );
            var proposed = leadershipTransferProposal.proposed();

            var proposedKnown = stream( replicationTargets( ctx ) )
                    .anyMatch( member -> member.equals( proposed ) );

            if ( !proposedKnown )
            {
                // TODO : I don't think we need to add groups here because its a local response
                handle( new RaftMessages.LeadershipTransfer.Rejection( ctx.myself(), commitIndex, commitIndexTerm, Set.of() ) );
            }

            // TODO : add groups
            var request = new RaftMessages.LeadershipTransfer.Request( ctx.myself(), commitIndex, commitIndexTerm, Set.of() );
            outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( proposed, request ) );
            return outcomeBuilder;
        }

        @Override
        public OutcomeBuilder handle(RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection) throws IOException
        {
            outcomeBuilder.addLeaderTransferRejection( leadershipTransferRejection );
            return outcomeBuilder;
        }

        private void stepDownToFollower(OutcomeBuilder outcomeBuilder, ReadableRaftState raftState )
        {
            outcomeBuilder.steppingDown( raftState.term() )
                    .setRole( Role.FOLLOWER )
                    .setLeader( null );
        }
    }
}
