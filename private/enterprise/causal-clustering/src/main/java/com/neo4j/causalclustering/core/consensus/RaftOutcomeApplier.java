/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static java.lang.String.format;

class RaftOutcomeApplier
{
    private final RaftState state;
    private final Log log;
    private final Consumer<RaftMessages.StatusResponse> statusResponseConsumer;
    private final Consumer<RaftMessages.LeadershipTransfer.Rejection> rejectionConsumer;
    private final Outbound<RaftMemberId,RaftMessages.RaftMessage> outbound;
    private final RaftMessageTimerResetMonitor raftMessageTimerResetMonitor;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final RaftLogShippingManager logShipping;
    private final RaftMembershipManager membershipManager;

    private volatile LeaderInfo leaderInfo;
    private final Collection<LeaderListener> leaderListeners = new ArrayList<>();

    RaftOutcomeApplier( RaftState state, Outbound<RaftMemberId,RaftMessages.RaftMessage> outbound, LeaderAvailabilityTimers leaderAvailabilityTimers,
                        RaftMessageTimerResetMonitor raftMessageTimerResetMonitor, RaftLogShippingManager logShipping, RaftMembershipManager membershipManager,
                        LogProvider logProvider, Consumer<RaftMessages.LeadershipTransfer.Rejection> rejectionConsumer,
                        Consumer<RaftMessages.StatusResponse> statusResponseConsumer )
    {
        this.state = state;
        this.outbound = outbound;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;
        this.raftMessageTimerResetMonitor = raftMessageTimerResetMonitor;
        this.logShipping = logShipping;
        this.membershipManager = membershipManager;
        this.log = logProvider.getLog( getClass() );
        this.rejectionConsumer = rejectionConsumer;
        this.statusResponseConsumer = statusResponseConsumer;
    }

    synchronized Role handle( Outcome outcome ) throws IOException
    {
        var newLeaderWasElected = leaderChanged( outcome, state.leader() );
        state.update( outcome ); // updates to raft log happen within
        sendMessages( outcome );

        handleTimers( outcome );
        handleLogShipping( outcome );

        leaderInfo = new LeaderInfo( outcome.getLeader(), outcome.getTerm() );

        if ( newLeaderWasElected )
        {
            notifyLeaderChanges( outcome );
        }

        driveMembership( outcome );

        outcome.getLeaderTransferRejection().ifPresent( rejection -> rejectionConsumer.accept( rejection ) );

        handleStatusResponse( outcome );
        return outcome.getRole();
    }

    private boolean leaderChanged( Outcome outcome, RaftMemberId oldLeader )
    {
        return !Objects.equals( oldLeader, outcome.getLeader() );
    }

    private void sendMessages( Outcome outcome )
    {
        for ( var outgoingMessage : outcome.getOutgoingMessages() )
        {
            try
            {
                outbound.send( outgoingMessage.to(), outgoingMessage.message() );
            }
            catch ( Exception e )
            {
                log.warn( format( "Failed to send message %s.", outgoingMessage ), e );
            }
        }
    }

    private void handleStatusResponse( Outcome outcome )
    {
        outcome.getStatusResponse().ifPresent( statusResponseConsumer );
    }

    private void handleTimers( Outcome outcome )
    {
        outcome.electionTimerChanged().ifPresentOrElse( electionTimerMode ->
                {
                    raftMessageTimerResetMonitor.timerReset();
                    leaderAvailabilityTimers.renewElectionTimer( electionTimerMode );
                }, () ->
                {
                    if ( outcome.isSteppingDown() )
                    {
                        raftMessageTimerResetMonitor.timerReset();
                    }
                } );
    }

    private void handleLogShipping( Outcome outcome )
    {
        var leaderContext = new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() );
        if ( outcome.isElectedLeader() )
        {
            logShipping.resume( leaderContext );
        }
        else if ( outcome.isSteppingDown() )
        {
            logShipping.pause();
        }

        if ( outcome.getRole() == LEADER )
        {
            logShipping.handleCommands( outcome.getShipCommands(), leaderContext );
        }
    }

    private void notifyLeaderChanges( Outcome outcome )
    {
        for ( var listener : leaderListeners )
        {
            outcome.stepDownTerm().ifPresent( listener::onLeaderStepDown );
            listener.onLeaderSwitch( new LeaderInfo( outcome.getLeader(), outcome.getTerm() ) );
        }
    }

    private void driveMembership( Outcome outcome ) throws IOException
    {
        membershipManager.processLog( outcome.getCommitIndex(), outcome.getLogCommands() );

        var nextRole = outcome.getRole();
        membershipManager.onRole( nextRole );

        if ( nextRole == LEADER )
        {
            membershipManager.handleLeadershipTransfers( state.areTransferringLeadership() );
            membershipManager.onFollowerStateChange( outcome.getFollowerStates() );
        }
    }

    Optional<LeaderInfo> getLeaderInfo()
    {
        return Optional.ofNullable( leaderInfo );
    }

    synchronized void registerListener( LeaderListener listener )
    {
        leaderListeners.add( listener );
        listener.onLeaderSwitch( state.leaderInfo() );
    }

    synchronized void unregisterListener( LeaderListener listener )
    {
        leaderListeners.remove( listener );
        listener.onUnregister();
    }
}
