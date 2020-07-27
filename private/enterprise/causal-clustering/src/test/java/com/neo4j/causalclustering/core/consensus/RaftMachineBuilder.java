/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.leader_transfer.ExpiringSet;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembers;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import com.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContext;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.replication.SendToMyself;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DurationRange;
import org.neo4j.io.state.StateStorage;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.configuration.ServerGroupsSupplier.listen;

public class RaftMachineBuilder
{
    private final MemberId member;
    private final int expectedClusterSize;
    private final RaftMembers.Builder memberSetBuilder;
    private final Clock clock;

    private TermState termState = new TermState();
    private StateStorage<TermState> termStateStorage = new InMemoryStateStorage<>( termState );
    private StateStorage<VoteState> voteStateStorage = new InMemoryStateStorage<>( new VoteState() );
    private RaftLog raftLog = new InMemoryRaftLog();
    private TimerService timerService;

    private Inbound<RaftMessages.InboundRaftMessageContainer<?>> inbound = handler ->
    {
    };
    private Outbound<MemberId,RaftMessages.RaftMessage> outbound = ( to, message, block ) ->
    {
    };

    private LogProvider logProvider = NullLogProvider.getInstance();

    private long term = termState.currentTerm();

    private DurationRange detectionWindow = DurationRange.parse( "480ms-540ms" );
    private long detectionWindowMin = detectionWindow.getMin().toMillis();
    private long heartbeatInterval = detectionWindowMin / RaftTimersConfig.HEARTBEAT_COUNT_IN_FAILURE_DETECTION;

    private long catchupTimeout = 30000;
    private long retryTimeMillis = detectionWindowMin / 2;
    private int catchupBatchSize = 64;
    private int maxAllowedShippingLag = 256;
    private StateStorage<RaftMembershipState> raftMembership =
            new InMemoryStateStorage<>( new RaftMembershipState() );
    private Monitors monitors = new Monitors();
    private CommitListener commitListener = commitIndex ->
    {
    };
    private InFlightCache inFlightCache = new ConsecutiveInFlightCache();

    public RaftMachineBuilder( MemberId member, int expectedClusterSize, RaftMembers.Builder memberSetBuilder, Clock clock )
    {
        this.member = member;
        this.expectedClusterSize = expectedClusterSize;
        this.memberSetBuilder = memberSetBuilder;
        this.clock = clock;
    }

    public RaftMachine build()
    {
        return buildFixture().raftMachine;
    }

    public RaftFixture buildFixture()
    {
        termState.update( term );
        var config = Config.newBuilder()
                           .set( CausalClusteringSettings.enable_pre_voting, false )
                           .set( CausalClusteringSettings.refuse_to_be_leader, false )
                           .set( CausalClusteringSettings.leader_failure_detection_window, detectionWindow )
                           .set( CausalClusteringSettings.election_failure_detection_window, detectionWindow )
                           .build();
        var raftTimersConfig = new RaftTimersConfig( config );
        LeaderAvailabilityTimers leaderAvailabilityTimers = new LeaderAvailabilityTimers( raftTimersConfig, clock, timerService, logProvider );
        SendToMyself leaderOnlyReplicator = new SendToMyself( member, outbound );
        RaftMembershipManager membershipManager = new RaftMembershipManager( leaderOnlyReplicator, member,
                                                                             memberSetBuilder, raftLog, logProvider, expectedClusterSize, detectionWindowMin,
                                                                             clock, catchupTimeout, raftMembership );
        membershipManager.setRecoverFromIndexSupplier( () -> 0 );
        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, timerService, clock, member, membershipManager,
                                            retryTimeMillis, catchupBatchSize, maxAllowedShippingLag, inFlightCache );

        var raftState = new RaftState( member, termStateStorage, membershipManager, raftLog, voteStateStorage, inFlightCache, logProvider,
                                       new ExpiringSet<>( Duration.ofMillis( 100 ), clock ) );
        var raftMessageHandlingContext = new RaftMessageHandlingContext( raftState, config, listen( config ), () -> false );
        var raftMessageTimerResetMonitor = monitors.newMonitor( RaftMessageTimerResetMonitor.class );
        var outcomeApplier = new RaftOutcomeApplier( raftState, outbound, leaderAvailabilityTimers, raftMessageTimerResetMonitor, logShipping,
                                                     membershipManager, logProvider, rejection ->
                                                     {
                                                     } );
        RaftMachine raft = new RaftMachine( member, leaderAvailabilityTimers, logProvider,
                                            membershipManager, inFlightCache, outcomeApplier, raftState, raftMessageHandlingContext );
        inbound.registerHandler( incomingMessage ->
                                 {
                                     try
                                     {
                                         ConsensusOutcome outcome = raft.handle( incomingMessage.message() );
                                         commitListener.notifyCommitted( outcome.getCommitIndex() );
                                     }
                                     catch ( IOException e )
                                     {
                                         throw new RuntimeException( e );
                                     }
                                 } );

        try
        {
            membershipManager.start();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return new RaftFixture( raft, logShipping, raftLog );
    }

    public RaftMachineBuilder electionTimeout( long electionTimeout )
    {
        this.detectionWindowMin = electionTimeout;
        return this;
    }

    public RaftMachineBuilder heartbeatInterval( long heartbeatInterval )
    {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public RaftMachineBuilder timerService( TimerService timerService )
    {
        this.timerService = timerService;
        return this;
    }

    public RaftMachineBuilder outbound( Outbound<MemberId,RaftMessages.RaftMessage> outbound )
    {
        this.outbound = outbound;
        return this;
    }

    public RaftMachineBuilder inbound( Inbound<RaftMessages.InboundRaftMessageContainer<?>> inbound )
    {
        this.inbound = inbound;
        return this;
    }

    public RaftMachineBuilder raftLog( RaftLog raftLog )
    {
        this.raftLog = raftLog;
        return this;
    }

    public RaftMachineBuilder inFlightCache( InFlightCache inFlightCache )
    {
        this.inFlightCache = inFlightCache;
        return this;
    }

    public RaftMachineBuilder commitListener( CommitListener commitListener )
    {
        this.commitListener = commitListener;
        return this;
    }

    RaftMachineBuilder monitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }

    public RaftMachineBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public interface CommitListener
    {
        /**
         * Called when the highest committed index increases.
         */
        void notifyCommitted( long commitIndex );
    }

    public static class RaftFixture
    {
        private final RaftMachine raftMachine;
        private final RaftLogShippingManager logShipping;
        private final RaftLog raftLog;

        public RaftFixture( RaftMachine raft, RaftLogShippingManager logShipping, RaftLog raftLog )
        {
            this.raftMachine = raft;
            this.logShipping = logShipping;
            this.raftLog = raftLog;
        }

        public RaftMachine raftMachine()
        {
            return raftMachine;
        }

        public RaftLog raftLog()
        {
            return raftLog;
        }

        public RaftLogShippingManager logShipping()
        {
            return logShipping;
        }
    }
}
