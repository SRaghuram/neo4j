/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;
import java.util.function.LongSupplier;

import org.neo4j.time.Clocks;

import static org.mockito.Mockito.verify;

public class LeaderAvailabilityHandlerTest
{
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> delegate = Mockito.mock( LifecycleMessageHandler.class );
    private LeaderAvailabilityTimers leaderAvailabilityTimers = Mockito.mock( LeaderAvailabilityTimers.class );
    private RaftId raftId = RaftIdFactory.random();
    private RaftMessageTimerResetMonitor raftMessageTimerResetMonitor = new DurationSinceLastMessageMonitor( Clocks.nanoClock() );
    private LongSupplier term = () -> 3;

    private LeaderAvailabilityHandler handler = new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, raftMessageTimerResetMonitor, term );

    private MemberId leader = new MemberId( UUID.randomUUID() );
    private RaftMessages.ReceivedInstantRaftIdAwareMessage<?> heartbeat =
            RaftMessages.ReceivedInstantRaftIdAwareMessage.of( Instant.now(), raftId, new RaftMessages.Heartbeat( leader, term.getAsLong(), 0, 0 ) );
    private RaftMessages.ReceivedInstantRaftIdAwareMessage<?> appendEntries =
            RaftMessages.ReceivedInstantRaftIdAwareMessage.of( Instant.now(), raftId,
                    new RaftMessages.AppendEntries.Request( leader, term.getAsLong(), 0, 0, RaftLogEntry.empty, 0 )
            );
    private RaftMessages.ReceivedInstantRaftIdAwareMessage<?> voteResponse =
            RaftMessages.ReceivedInstantRaftIdAwareMessage.of( Instant.now(), raftId, new RaftMessages.Vote.Response( leader, term.getAsLong(), false ) );

    @Test
    public void shouldRenewElectionForHeartbeats() throws Throwable
    {
        // given
        handler.start( raftId );

        // when
        handler.handle( heartbeat );

        // then
        verify( leaderAvailabilityTimers ).renewElection();
    }

    @Test
    public void shouldRenewElectionForAppendEntriesRequests() throws Throwable
    {
        // given
        handler.start( raftId );

        // when
        handler.handle( appendEntries );

        // then
        verify( leaderAvailabilityTimers ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionForOtherMessages() throws Throwable
    {
        // given
        handler.start( raftId );

        // when
        handler.handle( voteResponse );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForHeartbeatsFromEarlierTerm() throws Throwable
    {
        // given
        RaftMessages.ReceivedInstantRaftIdAwareMessage<?> heartbeat =  RaftMessages.ReceivedInstantRaftIdAwareMessage.of(
                Instant.now(), raftId, new RaftMessages.Heartbeat( leader, term.getAsLong() - 1, 0, 0 ) );

        handler.start( raftId );

        // when
        handler.handle( heartbeat );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForAppendEntriesRequestsFromEarlierTerms() throws Throwable
    {
        RaftMessages.ReceivedInstantRaftIdAwareMessage<?> appendEntries = RaftMessages.ReceivedInstantRaftIdAwareMessage.of(
                Instant.now(), raftId,
                new RaftMessages.AppendEntries.Request(
                        leader, term.getAsLong() - 1, 0, 0, RaftLogEntry.empty, 0 )
        );

        handler.start( raftId );

        // when
        handler.handle( appendEntries );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldDelegateStart() throws Throwable
    {
        // when
        handler.start( raftId );

        // then
        verify( delegate ).start( raftId );
    }

    @Test
    public void shouldDelegateStop() throws Throwable
    {
        // when
        handler.stop();

        // then
        verify( delegate ).stop();
    }
}
