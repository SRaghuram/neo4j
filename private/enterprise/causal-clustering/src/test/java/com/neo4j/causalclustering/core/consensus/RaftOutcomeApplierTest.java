/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RaftOutcomeApplierTest
{
    private RaftState raftState = mock( RaftState.class );
    private LogProvider logProvider = NullLogProvider.getInstance();
    private Outbound<MemberId,RaftMessages.RaftMessage> outbound = mock( Outbound.class );
    private RaftMessageTimerResetMonitor raftMessageTimerResetMonitor = mock( RaftMessageTimerResetMonitor.class );
    private LeaderAvailabilityTimers leaderAvailabilityTimers = mock( LeaderAvailabilityTimers.class );
    private RaftLogShippingManager logShipping = mock( RaftLogShippingManager.class );
    private RaftMembershipManager membershipManager = mock( RaftMembershipManager.class );

    private RaftOutcomeApplier raftOutcomeApplier =
            new RaftOutcomeApplier( raftState, outbound, leaderAvailabilityTimers, raftMessageTimerResetMonitor, logShipping, membershipManager, logProvider,
                                    rejection -> {}, c -> {} );

    private OutcomeBuilder outcomeTestBuilder = OutcomeTestBuilder.builder();

    @Test
    void shouldUpdateState() throws IOException
    {
        var outcome = outcomeTestBuilder.build();

        raftOutcomeApplier.handle( outcome );

        verify( raftState ).update( outcome );
    }

    @Test
    void shouldSendMessages() throws IOException
    {
        var outgoingMessages = Stream.generate( UUID::randomUUID )
                .map( MemberId::new )
                .map( member -> new RaftMessages.Directed( member, null ) )
                .limit( 3 )
                .collect( Collectors.toList() );

        for ( RaftMessages.Directed outgoingMessage : outgoingMessages )
        {
            outcomeTestBuilder.addOutgoingMessage( outgoingMessage ).build();
        }

        raftOutcomeApplier.handle( outcomeTestBuilder.build() );

        outgoingMessages.forEach( msg -> verify( outbound ).send( msg.to(), msg.message() ) );
    }

    @Test
    void shouldResetRaftMessageResetMonitorIfElectionRenewed() throws IOException
    {
        var outcome = outcomeTestBuilder.renewElectionTimer( ACTIVE_ELECTION ).build();

        raftOutcomeApplier.handle( outcome );

        verify( raftMessageTimerResetMonitor ).timerReset();
    }

    @Test
    void shouldRenewLeaderAvailabilityTimerIfSteppingDown() throws IOException
    {
        var outcome = outcomeTestBuilder.steppingDown( 3 ).build();

        raftOutcomeApplier.handle( outcome );

        verify( raftMessageTimerResetMonitor ).timerReset();
    }

    @Test
    void shouldNotResetRaftMessageResetMonitorIfElectionNotRenewedAndNotSteppingDown() throws IOException
    {
        var outcome = outcomeTestBuilder.build();

        raftOutcomeApplier.handle( outcome );

        verify( raftMessageTimerResetMonitor, never() ).timerReset();
    }

    @ParameterizedTest
    @EnumSource( ElectionTimerMode.class )
    void shouldRenewLeaderAvailabilityTimerAndResetRaftMessageResetMonitorIfElectionTimerModeSet( ElectionTimerMode mode ) throws IOException
    {
        var outcome = outcomeTestBuilder.renewElectionTimer( mode ).build();

        raftOutcomeApplier.handle( outcome );

        verify( leaderAvailabilityTimers ).renewElectionTimer( mode );
        verify( raftMessageTimerResetMonitor ).timerReset();
    }

    @Test
    void shouldNotRenewLeaderAvailabilityTimerIfElectionNotRenewed() throws IOException
    {
        var outcome = outcomeTestBuilder.build();

        raftOutcomeApplier.handle( outcome );

        verify( leaderAvailabilityTimers, never() ).renewElectionTimer( any() );
    }

    @Test
    void shouldResumeLogShippingIfElectedLeader() throws IOException
    {
        var outcome = outcomeTestBuilder
                .electedLeader()
                .setTerm( 23423L )
                .setLeaderCommit( 7589023470L )
                .build();

        raftOutcomeApplier.handle( outcome );

        verify( logShipping ).resume( new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() ) );
    }

    @Test
    void shouldNotResumeLogShippingIfNotElectedLeader() throws IOException
    {
        var outcome = outcomeTestBuilder
                .setTerm( 23423L )
                .setLeaderCommit( 7589023470L )
                .build();

        raftOutcomeApplier.handle( outcome );

        verify( logShipping, never() ).resume( any( LeaderContext.class ) );
    }

    @Test
    void shouldPauseIfSteppingDown() throws IOException
    {
        var outcome = outcomeTestBuilder.steppingDown( 0 ).build();

        raftOutcomeApplier.handle( outcome );

        verify( logShipping ).pause();
    }

    @Test
    void shouldNotPauseIfNotSteppingDown() throws IOException
    {
        var outcome = outcomeTestBuilder.build();

        raftOutcomeApplier.handle( outcome );

        verify( logShipping, never() ).pause();
    }

    @Test
    void shouldHandleLogShippingCommandsIfLeader() throws IOException
    {
        var outcome = outcomeTestBuilder
                .setRole( Role.LEADER )
                .setTerm( 78493L )
                .setLeaderCommit( 7589024379L )
                .build();

        raftOutcomeApplier.handle( outcome );

        verify( logShipping ).handleCommands( outcome.getShipCommands(), new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() ) );
    }

    @Test
    void shouldNotHandleLogShippingCommandsIfNotLeader() throws IOException
    {
        var outcome = outcomeTestBuilder
                .setRole( Role.FOLLOWER )
                .setTerm( 78493L )
                .setLeaderCommit( 7589024379L )
                .build();

        raftOutcomeApplier.handle( outcome );

        verify( logShipping, never() ).handleCommands( anyCollection(), any( LeaderContext.class ) );
    }

    @Test
    void shouldSetLeader() throws IOException
    {
        var outcome = outcomeTestBuilder.build();

        raftOutcomeApplier.handle( outcome );

        assertThat( raftOutcomeApplier.getLeaderInfo().map( LeaderInfo::memberId ), contains( outcome.getLeader() ) );
    }

    @Test
    void shouldNotifyLeaderChangesIfNewLeader() throws IOException
    {
        when( raftState.leader() ).thenReturn( new MemberId( UUID.randomUUID() ) );
        var outcome = outcomeTestBuilder.build();
        var listener = mock( LeaderListener.class );
        raftOutcomeApplier.registerListener( listener );

        raftOutcomeApplier.handle( outcome );

        verifyListener( outcome, listener );
    }

    @Test
    void shouldNotNotifyLeaderChangesIfNoNewLeader() throws IOException
    {
        MemberId leader = new MemberId( UUID.randomUUID() );
        when( raftState.leader() ).thenReturn( leader );
        var outcome = outcomeTestBuilder.setLeader( leader ).build();
        var listener = mock( LeaderListener.class );
        raftOutcomeApplier.registerListener( listener );

        raftOutcomeApplier.handle( outcome );

        verify( listener, never() ).onLeaderSwitch( any( LeaderInfo.class ) );
        verify( listener, never() ).onLeaderStepDown( anyLong() );
    }

    @Test
    void shouldNotifyLeaderChangesIfNullNewLeader() throws IOException
    {
        MemberId leader = new MemberId( UUID.randomUUID() );
        when( raftState.leader() ).thenReturn( leader );
        var outcome = outcomeTestBuilder.setLeader( null ).build();
        var listener = mock( LeaderListener.class );
        raftOutcomeApplier.registerListener( listener );

        raftOutcomeApplier.handle( outcome );

        verifyListener( outcome, listener );
    }

    private void verifyListener( Outcome outcome, LeaderListener listener )
    {
        if ( outcome.stepDownTerm().isPresent() )
        {
            verify( listener ).onLeaderStepDown( outcome.stepDownTerm().getAsLong() );
        }
        verify( listener ).onLeaderSwitch( any( LeaderInfo.class ) );
    }

    @Test
    void shouldNotifyLeaderChangesIfNullOldLeader() throws IOException
    {
        MemberId leader = new MemberId( UUID.randomUUID() );
        when( raftState.leader() ).thenReturn( null );
        var outcome = outcomeTestBuilder.setLeader( leader ).build();
        var listener = mock( LeaderListener.class );
        raftOutcomeApplier.registerListener( listener );

        raftOutcomeApplier.handle( outcome );

        verifyListener( outcome, listener );
    }

    @Test
    void shouldDriveMembership() throws IOException
    {
        var outcome = outcomeTestBuilder.setCommitIndex( 78798L ).build();

        raftOutcomeApplier.handle( outcome );

        verify( membershipManager ).processLog( outcome.getCommitIndex(), outcome.getLogCommands() );
        verify( membershipManager ).onRole( outcome.getRole() );
    }

    @Test
    void shouldDriveMembershipFollowerStateIfLeader() throws IOException
    {
        var outcome = outcomeTestBuilder.setRole( Role.LEADER ).build();

        raftOutcomeApplier.handle( outcome );

        verify( membershipManager ).onFollowerStateChange( outcome.getFollowerStates() );
    }

    @Test
    void shouldNotDriveMembershipFollowerStateIfNotLeader() throws IOException
    {
        var outcome = outcomeTestBuilder.setRole( Role.FOLLOWER ).build();

        raftOutcomeApplier.handle( outcome );

        verify( membershipManager, never() ).onFollowerStateChange( any( FollowerStates.class ) );
    }

    @Test
    void shouldChangeMembershipManagerStateIfTransferringLeadership() throws IOException
    {
        var transferTarget = new MemberId( UUID.randomUUID() );
        var outcome = outcomeTestBuilder.setRole( Role.LEADER )
                                        .startTransferringLeadership( transferTarget )
                                        .build();
        when( raftState.areTransferringLeadership() ).thenReturn( true );

        raftOutcomeApplier.handle( outcome );

        verify( membershipManager ).handleLeadershipTransfers( true );
    }

    @Test
    void shouldNotChangeMembershipManagerStateIfNextRoleIsNotLeader() throws IOException
    {
        var transferTarget = new MemberId( UUID.randomUUID() );
        var outcome = outcomeTestBuilder.setRole( Role.FOLLOWER )
                                        .startTransferringLeadership( transferTarget )
                                        .build();
        when( raftState.areTransferringLeadership() ).thenReturn( true );

        raftOutcomeApplier.handle( outcome );

        verify( membershipManager, never() ).handleLeadershipTransfers( true );
    }

    @Test
    void shouldReturnNextRole() throws IOException
    {
        var outcome = outcomeTestBuilder.setRole( Role.CANDIDATE ).build();

        var role = raftOutcomeApplier.handle( outcome );

        assertThat( role, is( outcome.getRole() ) );
    }
}
