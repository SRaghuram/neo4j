/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.function.LongSupplier;

import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.FAILURE_DETECTION;
import static org.mockito.Mockito.verify;

class LeaderAvailabilityHandlerTest
{
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>> delegate = Mockito.mock( LifecycleMessageHandler.class );
    private LeaderAvailabilityTimers leaderAvailabilityTimers = Mockito.mock( LeaderAvailabilityTimers.class );
    private RaftGroupId raftGroupId = IdFactory.randomRaftId();
    private RaftMessageTimerResetMonitor raftMessageTimerResetMonitor = new DurationSinceLastMessageMonitor( Clocks.nanoClock() );
    private LongSupplier term = () -> 3;

    private LeaderAvailabilityHandler handler = new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, raftMessageTimerResetMonitor, term );

    private RaftMemberId leader = IdFactory.randomRaftMemberId();
    private RaftMessages.InboundRaftMessageContainer<?> heartbeat =
            RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftGroupId, new RaftMessages.Heartbeat( leader, term.getAsLong(), 0, 0 ) );
    private RaftMessages.InboundRaftMessageContainer<?> appendEntries =
            RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftGroupId,
                                                         new RaftMessages.AppendEntries.Request( leader, term.getAsLong(), 0, 0, RaftLogEntry.empty, 0 )
            );
    private RaftMessages.InboundRaftMessageContainer<?> voteResponse =
            RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftGroupId, new RaftMessages.Vote.Response( leader, term.getAsLong(), false ) );

    @Test
    void shouldRenewElectionForHeartbeats() throws Throwable
    {
        // given
        handler.start( raftGroupId );

        // when
        handler.handle( heartbeat );

        // then
        verify( leaderAvailabilityTimers ).renewElectionTimer( FAILURE_DETECTION );
    }

    @Test
    void shouldRenewElectionForAppendEntriesRequests() throws Throwable
    {
        // given
        handler.start( raftGroupId );

        // when
        handler.handle( appendEntries );

        // then
        verify( leaderAvailabilityTimers ).renewElectionTimer( FAILURE_DETECTION );
    }

    @Test
    void shouldNotRenewElectionForOtherMessages() throws Throwable
    {
        // given
        handler.start( raftGroupId );

        // when
        handler.handle( voteResponse );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElectionTimer( FAILURE_DETECTION );
    }

    @Test
    void shouldNotRenewElectionTimeoutsForHeartbeatsFromEarlierTerm() throws Throwable
    {
        // given
        RaftMessages.InboundRaftMessageContainer<?> heartbeat =  RaftMessages.InboundRaftMessageContainer.of(
                Instant.now(), raftGroupId, new RaftMessages.Heartbeat( leader, term.getAsLong() - 1, 0, 0 ) );

        handler.start( raftGroupId );

        // when
        handler.handle( heartbeat );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElectionTimer( FAILURE_DETECTION );
    }

    @Test
    void shouldNotRenewElectionTimeoutsForAppendEntriesRequestsFromEarlierTerms() throws Throwable
    {
        RaftMessages.InboundRaftMessageContainer<?> appendEntries = RaftMessages.InboundRaftMessageContainer.of(
                Instant.now(), raftGroupId,
                new RaftMessages.AppendEntries.Request(
                        leader, term.getAsLong() - 1, 0, 0, RaftLogEntry.empty, 0 )
        );

        handler.start( raftGroupId );

        // when
        handler.handle( appendEntries );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElectionTimer( FAILURE_DETECTION );
    }

    @Test
    void shouldDelegateStart() throws Throwable
    {
        // when
        handler.start( raftGroupId );

        // then
        verify( delegate ).start( raftGroupId );
    }

    @Test
    void shouldDelegateStop() throws Throwable
    {
        // when
        handler.stop();

        // then
        verify( delegate ).stop();
    }
}
