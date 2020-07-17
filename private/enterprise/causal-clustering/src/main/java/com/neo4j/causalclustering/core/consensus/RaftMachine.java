/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContext;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.FAILURE_DETECTION;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;

/**
 * Implements the Raft Consensus Algorithm.
 * <p>
 * The algorithm is driven by incoming messages provided to {@link #handle}.
 */
public class RaftMachine implements LeaderLocator, CoreMetaData, DatabasePanicEventHandler, RoleProvider
{
    private final InFlightCache inFlightCache;
    private final RaftOutcomeApplier outcomeApplier;

    private final RaftState state;
    private final RaftMessageHandlingContext messageHandlingContext;
    private final MemberId myself;

    private final LeaderAvailabilityTimers leaderAvailabilityTimers;
    private final RaftMembershipManager membershipManager;

    private final Log log;
    private volatile Role currentRole = Role.FOLLOWER;

    public RaftMachine( MemberId myself, LeaderAvailabilityTimers leaderAvailabilityTimers, LogProvider logProvider, RaftMembershipManager membershipManager,
            InFlightCache inFlightCache, RaftOutcomeApplier outcomeApplier, RaftState state, RaftMessageHandlingContext messageHandlingContext )
    {
        this.myself = myself;
        this.leaderAvailabilityTimers = leaderAvailabilityTimers;

        this.log = logProvider.getLog( getClass() );

        this.membershipManager = membershipManager;

        this.inFlightCache = inFlightCache;
        this.outcomeApplier = outcomeApplier;
        this.state = state;
        this.messageHandlingContext = messageHandlingContext;
    }

    @Override
    public void onPanic( Throwable cause )
    {
        stopTimers();
    }

    /**
     * This should be called after the major recovery operations are complete. Before this is called
     * this instance cannot become a leader (the timers are disabled) and entries will not be cached
     * in the in-flight map, because the application process is not running and ready to consume them.
     */
    public void postRecoveryActions()
    {
        if ( outcomeApplier.getLeaderInfo().isPresent() )
        {
            leaderAvailabilityTimers.renewElectionTimer( FAILURE_DETECTION );
        }
        leaderAvailabilityTimers.start( this::electionTimeout, this::triggerHeartbeat );
        state.setTimersStarted();
        inFlightCache.enable();
    }

    public void stopTimers()
    {
        leaderAvailabilityTimers.stop();
    }

    private void electionTimeout() throws IOException
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

    private void triggerHeartbeat() throws IOException
    {
        handle( new RaftMessages.Timeout.Heartbeat( myself ) );
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
    public Optional<LeaderInfo> getLeaderInfo()
    {
        return outcomeApplier.getLeaderInfo();
    }

    @Override
    public void registerListener( LeaderListener listener )
    {
        outcomeApplier.registerListener( listener );
    }

    @Override
    public void unregisterListener( LeaderListener listener )
    {
        outcomeApplier.unregisterListener( listener );
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

    public synchronized ConsensusOutcome handle( RaftMessages.RaftMessage incomingMessage ) throws IOException
    {
        Outcome outcome = currentRole.handler.handle( incomingMessage, messageHandlingContext, log );

        currentRole = outcomeApplier.handle( outcome );

        return outcome;
    }

    @Override
    public boolean isLeader()
    {
        return currentRole == LEADER;
    }

    @Override
    public Role currentRole()
    {
        return currentRole;
    }

    public MemberId memberId()
    {
        return myself;
    }

    @Override
    public String toString()
    {
        return "RaftMachine{" +
               "myself=" + myself +
               ", currentRole=" + currentRole +
               ", term=" + term() +
               ", votingMembers=" + votingMembers() +
               ", leader=" + outcomeApplier.getLeaderInfo() +
               '}';
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

    public enum Timeouts implements TimerService.TimerName
    {
        ELECTION,
        HEARTBEAT
    }
}
