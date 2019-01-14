/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.function.LongSupplier;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;

public class LeaderAvailabilityHandler implements LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegateHandler;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final ShouldRenewElectionTimeout shouldRenewElectionTimeout;
    private final RaftMessageTimerResetMonitor raftMessageTimerResetMonitor;

    public LeaderAvailabilityHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegateHandler,
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
    public synchronized void start( ClusterId clusterId ) throws Throwable
    {
        delegateHandler.start( clusterId );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        delegateHandler.stop();
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
    {
        handleTimeouts( message );
        delegateHandler.handle( message );
    }

    private void handleTimeouts( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
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
