/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.RaftLogHelper;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheMonitor;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.consensus.schedule.OnDemandTimerService;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts.ELECTION;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class RaftMachineTest
{
    private final int electionTimeout = 500;
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );
    private MemberId member3 = member( 3 );
    private MemberId member4 = member( 4 );

    private ReplicatedInteger data1 = ReplicatedInteger.valueOf( 1 );
    private ReplicatedInteger data2 = ReplicatedInteger.valueOf( 2 );

    private RaftLog raftLog = new InMemoryRaftLog();

    @Test
    void shouldAlwaysStartAsFollower()
    {
        // when
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .build();

        // then
        Assertions.assertEquals( FOLLOWER, raft.currentRole() );
    }

    @Test
    void shouldRequestVotesOnElectionTimeout() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .electionTimeout( electionTimeout )
                .clock( fakeClock )
                .outbound( messages )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        timerService.invoke( ELECTION );

        // Then
        assertThat( messages.sentTo( myself ).size(), equalTo( 0 ) );

        assertThat( messages.sentTo( member1 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member1 ).get( 0 ), instanceOf( RaftMessages.Vote.Request.class ) );

        assertThat( messages.sentTo( member2 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member2 ).get( 0 ), instanceOf( RaftMessages.Vote.Request.class ) );
    }

    @Test
    void shouldBecomeLeaderInMajorityOf3() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );
        assertThat( raft.isLeader(), is( false ) );

        // When
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( true ) );
    }

    @Test
    void shouldBecomeLeaderInMajorityOf5() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState(
                new MembershipEntry( 0, asSet( myself, member1, member2, member3, member4 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );

        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );
        assertThat( raft.isLeader(), is( false ) );

        // When
        raft.handle( voteResponse().from( member2 ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( true ) );
    }

    @Test
    void shouldNotBecomeLeaderOnMultipleVotesFromSameMember() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState(
                new MembershipEntry( 0, asSet( myself, member1, member2, member3, member4 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    void shouldNotBecomeLeaderWhenVotingOnItself() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );

        // When
        raft.handle( voteResponse().from( myself ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    void shouldNotBecomeLeaderWhenMembersVoteNo() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 1 ).deny().build() );
        raft.handle( voteResponse().from( member2 ).term( 1 ).deny().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    void shouldNotBecomeLeaderByVotesFromOldTerm() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );
        // When
        raft.handle( voteResponse().from( member1 ).term( 0 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 0 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    void shouldVoteFalseForCandidateInOldTerm() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .clock( fakeClock )
                .outbound( messages )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        raft.handle( voteRequest().from( member1 ).term( -1 ).candidate( member1 )
                .lastLogIndex( 0 ).lastLogTerm( -1 ).build() );

        // Then
        assertThat( messages.sentTo( member1 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member1 ), hasItem( voteResponse().from( myself ).term( 0 ).deny().build() ) );
    }

    @Test
    void shouldNotBecomeLeaderByVotesFromFutureTerm() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 2 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 2 ).grant().build() );

        assertThat( raft.isLeader(), is( false ) );
        Assertions.assertEquals( raft.term(), 2L );
    }

    @Test
    void shouldAppendNewLeaderBarrierAfterBecomingLeader() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .clock( fakeClock )
                .outbound( messages )
                .raftLog( raftLog )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        timerService.invoke( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        Assertions.assertEquals( new NewLeaderBarrier(), RaftLogHelper.readLogEntry( raftLog, raftLog.appendIndex() ).content() );
    }

    @Test
    void leaderShouldSendHeartBeatsOnHeartbeatTimeout() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .outbound( messages )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timerService.invoke( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // When
        timerService.invoke( RaftMachine.Timeouts.HEARTBEAT );

        // Then
        Assertions.assertTrue( last( messages.sentTo( member1 ) ) instanceof RaftMessages.Heartbeat );
        Assertions.assertTrue( last( messages.sentTo( member2 ) ) instanceof RaftMessages.Heartbeat );
    }

    @Test
    void shouldReturnNullIfReceivesClientRequestWithNoLeaderElected() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        // There is no leader
        // Then
        Assertions.assertNull( raft.getLeaderInfo() );
    }

    @Test
    void shouldPersistAtSpecifiedLogIndex() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .clock( fakeClock )
                .raftLog( raftLog )
                .build();

        raftLog.append( new RaftLogEntry( 0, new MemberIdSet( asSet( myself, member1, member2 ) ) ) );

        // when
        raft.handle( appendEntriesRequest().from( member1 ).prevLogIndex( 0 ).prevLogTerm( 0 ).leaderTerm( 0 )
                .logEntry( new RaftLogEntry( 0, data1 ) ).build());
        // then
        Assertions.assertEquals( 1, raftLog.appendIndex() );
        Assertions.assertEquals( data1, RaftLogHelper.readLogEntry( raftLog, 1 ).content() );
    }

    @Test
    void newMembersShouldBeIncludedInHeartbeatMessages() throws Exception
    {
        // Given
        DirectNetworking network = new DirectNetworking();
        final MemberId newMember = member( 99 );
        DirectNetworking.Inbound newMemberInbound = network.new Inbound( newMember );
        final OutboundMessageCollector messages = new OutboundMessageCollector();
        newMemberInbound.registerHandler( message -> messages.send( newMember, message.message() ) );

        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .outbound( messages )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // We make ourselves the leader
        timerService.invoke( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // When
        raft.setTargetMembershipSet( asSet( myself, member1, member2, newMember ) );
        network.processMessages();

        timerService.invoke( RaftMachine.Timeouts.HEARTBEAT );
        network.processMessages();

        // Then
        Assertions.assertEquals( RaftMessages.AppendEntries.Request.class, messages.sentTo( newMember ).get( 0 ).getClass() );
    }

    @Test
    void shouldMonitorLeaderNotFound() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timerService( timerService )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );

        // When
        // There is no leader
        // Then
        Assertions.assertNull( raft.getLeaderInfo() );
    }

    @Test
    void shouldNotCacheInFlightEntriesUntilAfterRecovery() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        InFlightCache inFlightCache = new ConsecutiveInFlightCache( 10, 10000, InFlightCacheMonitor.VOID, false );
        OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
        RaftMachine raft =
                new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE ).timerService( timerService )
                        .clock( fakeClock ).raftLog( raftLog ).inFlightCache( inFlightCache ).build();

        raftLog.append( new RaftLogEntry( 0, new MemberIdSet( asSet( myself, member1, member2 ) ) ) );

        // when
        raft.handle( appendEntriesRequest().from( member1 ).prevLogIndex( 0 ).prevLogTerm( 0 ).leaderTerm( 0 )
                .logEntry( new RaftLogEntry( 0, data1 ) ).build() );

        // then
        Assertions.assertEquals( data1, RaftLogHelper.readLogEntry( raftLog, 1 ).content() );
        Assertions.assertNull( inFlightCache.get( 1L ) );

        // when
        raft.postRecoveryActions();
        raft.handle( appendEntriesRequest().from( member1 ).prevLogIndex( 1 ).prevLogTerm( 0 ).leaderTerm( 0 )
                .logEntry( new RaftLogEntry( 0, data2 ) ).build() );

        // then
        Assertions.assertEquals( data2, RaftLogHelper.readLogEntry( raftLog, 2 ).content() );
        Assertions.assertEquals( data2, inFlightCache.get( 2L ).content() );
    }
}
