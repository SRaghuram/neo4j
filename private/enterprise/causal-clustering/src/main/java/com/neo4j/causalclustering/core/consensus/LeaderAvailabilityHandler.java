/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.function.LongSupplier;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.FAILURE_DETECTION;
import static java.lang.Boolean.FALSE;

public class LeaderAvailabilityHandler implements LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
{
    private final LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> delegateHandler;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final ShouldRenewElectionTimeout shouldRenewElectionTimeout;
    private final RaftMessageTimerResetMonitor raftMessageTimerResetMonitor;

    public LeaderAvailabilityHandler( LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> delegateHandler,
            LeaderAvailabilityTimers leaderAvailabilityTimers, RaftMessageTimerResetMonitor raftMessageTimerResetMonitor, LongSupplier term )
    {
        this.delegateHandler = delegateHandler;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;
        this.shouldRenewElectionTimeout = new ShouldRenewElectionTimeout( term );
        this.raftMessageTimerResetMonitor = raftMessageTimerResetMonitor;
    }

    public static ComposableMessageHandler composable( LeaderAvailabilityTimers leaderAvailabilityTimers,
            RaftMessageTimerResetMonitor raftMessageTimerResetMonitor, LongSupplier term )
    {
        return delegate -> new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, raftMessageTimerResetMonitor, term );
    }

    @Override
    public synchronized void start( RaftGroupId raftGroupId ) throws Exception
    {
        delegateHandler.start( raftGroupId );
    }

    @Override
    public synchronized void stop() throws Exception
    {
        delegateHandler.stop();
    }

    @Override
    public void handle( RaftMessages.InboundRaftMessageContainer<?> message )
    {
        handleTimeouts( message );
        delegateHandler.handle( message );
    }

    private void handleTimeouts( RaftMessages.InboundRaftMessageContainer<?> message )
    {
        if ( message.message().dispatch( shouldRenewElectionTimeout ) )
        {
            raftMessageTimerResetMonitor.timerReset();
            leaderAvailabilityTimers.renewElectionTimer( FAILURE_DETECTION );
        }
    }

    private static class ShouldRenewElectionTimeout implements RaftMessages.Handler<Boolean, RuntimeException>
    {
        private final LongSupplier term;

        private ShouldRenewElectionTimeout( LongSupplier term )
        {
            this.term = term;
        }

        @Override
        public Boolean handle( RaftMessages.AppendEntries.Request request )
        {
            return request.leaderTerm() >= term.getAsLong();
        }

        @Override
        public Boolean handle( RaftMessages.Heartbeat heartbeat )
        {
            return heartbeat.leaderTerm() >= term.getAsLong();
        }

        @Override
        public Boolean handle( RaftMessages.Vote.Request request )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Vote.Response response )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Request request )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Response response )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.AppendEntries.Response response )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Election election )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.Request request )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PruneRequest pruneRequest )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
        {
            return FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.StatusResponse statusResponse ) throws RuntimeException
        {
            return FALSE;
        }
    }
}
