/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.helper.VolatileFuture;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static java.lang.String.format;

class RaftOutcomeApplier implements LeaderLocator
{
    private final RaftState state;
    private final Log log;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final RaftMessageTimerResetMonitor raftMessageTimerResetMonitor;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final RaftLogShippingManager logShipping;
    private final RaftMembershipManager membershipManager;

    private final VolatileFuture<MemberId> volatileLeader = new VolatileFuture<>( null );
    private final Collection<LeaderListener> leaderListeners = new ArrayList<>();

    RaftOutcomeApplier( RaftState state, Outbound<MemberId,RaftMessages.RaftMessage> outbound, LeaderAvailabilityTimers leaderAvailabilityTimers,
            RaftMessageTimerResetMonitor raftMessageTimerResetMonitor, RaftLogShippingManager logShipping, RaftMembershipManager membershipManager,
            LogProvider logProvider )
    {
        this.state = state;
        this.outbound = outbound;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;
        this.raftMessageTimerResetMonitor = raftMessageTimerResetMonitor;
        this.logShipping = logShipping;
        this.membershipManager = membershipManager;
        this.log = logProvider.getLog( getClass() );
    }

    synchronized Role handle( Outcome outcome ) throws IOException
    {
        var newLeaderWasElected = leaderChanged( outcome, state.leader() );
        state.update( outcome ); // updates to raft log happen within
        sendMessages( outcome );

        handleTimers( outcome );
        handleLogShipping( outcome );

        volatileLeader.set( outcome.getLeader() );

        if ( newLeaderWasElected )
        {
            notifyLeaderChanges( outcome );
        }

        driveMembership( outcome );

        return outcome.getRole();
    }

    private boolean leaderChanged( Outcome outcome, MemberId oldLeader )
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

    private void handleTimers( Outcome outcome )
    {
        if ( outcome.electionTimeoutRenewed() )
        {
            raftMessageTimerResetMonitor.timerReset();
            leaderAvailabilityTimers.renewElection();
        }
        else if ( outcome.isSteppingDown() )
        {
            raftMessageTimerResetMonitor.timerReset();
        }
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
            listener.onLeaderEvent( outcome );
        }
    }

    private void driveMembership( Outcome outcome ) throws IOException
    {
        membershipManager.processLog( outcome.getCommitIndex(), outcome.getLogCommands() );

        var nextRole = outcome.getRole();
        membershipManager.onRole( nextRole );

        if ( nextRole == LEADER )
        {
            membershipManager.onFollowerStateChange( outcome.getFollowerStates() );
        }
    }

    @Override
    public MemberId getLeader() throws NoLeaderFoundException
    {
        try
        {
            return volatileLeader.get( 0L, Objects::nonNull );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new NoLeaderFoundException( e );
        }
        catch ( TimeoutException e )
        {
            throw new NoLeaderFoundException( e );
        }
    }

    @Override
    public synchronized void registerListener( LeaderListener listener )
    {
        leaderListeners.add( listener );
        listener.onLeaderSwitch( state.leaderInfo() );
    }

    @Override
    public synchronized void unregisterListener( LeaderListener listener )
    {
        leaderListeners.remove( listener );
    }

    @VisibleForTesting
    RaftLogShippingManager logShippingManager()
    {
        return logShipping;
    }
}
