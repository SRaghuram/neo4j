/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.error_handling.PanicEventHandler;
import com.neo4j.causalclustering.helper.VolatileFuture;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static java.lang.String.format;

/**
 * Implements the Raft Consensus Algorithm.
 * <p>
 * The algorithm is driven by incoming messages provided to {@link #handle}.
 */
public class RaftMachine implements LeaderLocator, CoreMetaData, PanicEventHandler
{
    private final RaftMessageTimerResetMonitor raftMessageTimerResetMonitor;
    private InFlightCache inFlightCache;

    @Override
    public void onPanic()
    {
        stopTimers();
    }

    public enum Timeouts implements TimerService.TimerName
    {
        ELECTION,
        HEARTBEAT
    }

    private final RaftState state;
    private final MemberId myself;

    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private RaftMembershipManager membershipManager;

    private final VolatileFuture<MemberId> volatileLeader = new VolatileFuture<>( null );

    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final Log log;
    private volatile Role currentRole = Role.FOLLOWER;

    private RaftLogShippingManager logShipping;

    public RaftMachine( MemberId myself, StateStorage<TermState> termStorage, StateStorage<VoteState> voteStorage, RaftLog entryLog,
            LeaderAvailabilityTimers leaderAvailabilityTimers, Outbound<MemberId,RaftMessages.RaftMessage> outbound, LogProvider logProvider,
            RaftMembershipManager membershipManager, RaftLogShippingManager logShipping, InFlightCache inFlightCache, boolean refuseToBecomeLeader,
            boolean supportPreVoting, Monitors monitors )
    {
        this.myself = myself;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;

        this.outbound = outbound;
        this.logShipping = logShipping;
        this.log = logProvider.getLog( getClass() );

        this.membershipManager = membershipManager;

        this.inFlightCache = inFlightCache;
        this.state = new RaftState( myself, termStorage, membershipManager, entryLog, voteStorage, inFlightCache,
                logProvider, supportPreVoting, refuseToBecomeLeader );

        raftMessageTimerResetMonitor = monitors.newMonitor( RaftMessageTimerResetMonitor.class );
    }

    /**
     * This should be called after the major recovery operations are complete. Before this is called
     * this instance cannot become a leader (the timers are disabled) and entries will not be cached
     * in the in-flight map, because the application process is not running and ready to consume them.
     */
    public synchronized void postRecoveryActions()
    {
        leaderAvailabilityTimers.start( this::electionTimeout, () -> handle( new RaftMessages.Timeout.Heartbeat( myself ) ) );
        inFlightCache.enable();
    }

    public synchronized void stopTimers()
    {
        leaderAvailabilityTimers.stop();
    }

    private synchronized void electionTimeout() throws IOException
    {
        if ( leaderAvailabilityTimers.isElectionTimedOut() )
        {
            triggerElection();
        }
    }

    public void triggerElection() throws IOException
    {
        handle( new RaftMessages.Timeout.Election( myself ) );
    }

    public synchronized RaftCoreState coreState()
    {
        return new RaftCoreState( membershipManager.getCommitted() );
    }

    public synchronized void installCoreState( RaftCoreState coreState ) throws IOException
    {
        membershipManager.install( coreState.committed() );
    }

    public synchronized void setTargetMembershipSet( Set<MemberId> targetMembers )
    {
        membershipManager.setTargetMembershipSet( targetMembers );

        if ( currentRole == LEADER )
        {
            membershipManager.onFollowerStateChange( state.followerStates() );
        }
    }

    @Override
    public MemberId getLeader() throws NoLeaderFoundException
    {
        return waitForLeader( 0, Objects::nonNull );
    }

    private MemberId waitForLeader( long timeoutMillis, Predicate<MemberId> predicate ) throws NoLeaderFoundException
    {
        try
        {
            return volatileLeader.get( timeoutMillis, predicate );
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

    private Collection<LeaderListener> leaderListeners = new ArrayList<>();

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

    /**
     * Every call to state() gives you an immutable copy of the current state.
     *
     * @return A fresh view of the state.
     */
    public synchronized ExposedRaftState state()
    {
        return state.copy();
    }

    private void notifyLeaderChanges( Outcome outcome )
    {
        for ( LeaderListener listener : leaderListeners )
        {
            listener.onLeaderEvent( outcome );
        }
    }

    private void handleLogShipping( Outcome outcome )
    {
        LeaderContext leaderContext = new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() );
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

    private boolean leaderChanged( Outcome outcome, MemberId oldLeader )
    {
        return !Objects.equals( oldLeader, outcome.getLeader() );
    }

    public synchronized ConsensusOutcome handle( RaftMessages.RaftMessage incomingMessage ) throws IOException
    {
        Outcome outcome = currentRole.handler.handle( incomingMessage, state, log );

        boolean newLeaderWasElected = leaderChanged( outcome, state.leader() );

        state.update( outcome ); // updates to raft log happen within
        sendMessages( outcome );

        handleTimers( outcome );
        handleLogShipping( outcome );

        driveMembership( outcome );

        volatileLeader.set( outcome.getLeader() );

        if ( newLeaderWasElected )
        {
            notifyLeaderChanges( outcome );
        }
        return outcome;
    }

    private void driveMembership( Outcome outcome ) throws IOException
    {
        membershipManager.processLog( outcome.getCommitIndex(), outcome.getLogCommands() );

        currentRole = outcome.getRole();
        membershipManager.onRole( currentRole );

        if ( currentRole == LEADER )
        {
            membershipManager.onFollowerStateChange( state.followerStates() );
        }
    }

    private void handleTimers( Outcome outcome )
    {
        if ( outcome.electionTimeoutRenewed() )
        {
            raftMessageTimerResetMonitor.timerReset();
            leaderAvailabilityTimers.renewElection();
        }
    }

    private void sendMessages( Outcome outcome )
    {
        for ( RaftMessages.Directed outgoingMessage : outcome.getOutgoingMessages() )
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

    @Override
    public boolean isLeader()
    {
        return currentRole == LEADER;
    }

    public Role currentRole()
    {
        return currentRole;
    }

    public MemberId identity()
    {
        return myself;
    }

    public RaftLogShippingManager logShippingManager()
    {
        return logShipping;
    }

    @Override
    public String toString()
    {
        return format( "RaftInstance{role=%s, term=%d, currentMembers=%s}", currentRole, term(), votingMembers() );
    }

    public long term()
    {
        return state.term();
    }

    public Set<MemberId> votingMembers()
    {
        return membershipManager.votingMembers();
    }

    public Set<MemberId> replicationMembers()
    {
        return membershipManager.replicationMembers();
    }
}
