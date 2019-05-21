/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.function.LongSupplier;

public class LeaderAvailabilityHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> delegateHandler;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final ShouldRenewElectionTimeout shouldRenewElectionTimeout;
    private final RaftMessageTimerResetMonitor raftMessageTimerResetMonitor;

    public LeaderAvailabilityHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> delegateHandler,
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
    public synchronized void start( RaftId raftId ) throws Exception
    {
        delegateHandler.start( raftId );
    }

    @Override
    public synchronized void stop() throws Exception
    {
        delegateHandler.stop();
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> message )
    {
        handleTimeouts( message );
        delegateHandler.handle( message );
    }

    private void handleTimeouts( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> message )
    {
        if ( message.dispatch( shouldRenewElectionTimeout ) )
        {
            raftMessageTimerResetMonitor.timerReset();
            leaderAvailabilityTimers.renewElection();
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
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Vote.Response response )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Request request )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PreVote.Response response )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.AppendEntries.Response response )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Election election )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.Request request )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return Boolean.FALSE;
        }

        @Override
        public Boolean handle( RaftMessages.PruneRequest pruneRequest )
        {
            return Boolean.FALSE;
        }
    }
}
