/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.EnterpriseCoreEditionModule;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.MonitoredRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheFactory;
import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategy;
import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import org.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetBuilder;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.term.MonitoredTermStateStorage;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.SendToMyself;
import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.causalclustering.core.state.CoreStateStorageService;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.RaftCoreTopologyConnector;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.catchup_batch_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.join_catch_up_timeout;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.log_shipping_max_lag;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_MEMBERSHIP;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_TERM;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_VOTE;
import static org.neo4j.time.Clocks.systemClock;

public class ConsensusModule
{
    private final MonitoredRaftLog raftLog;
    private final RaftMachine raftMachine;
    private final RaftMembershipManager raftMembershipManager;
    private final InFlightCache inFlightCache;

    private final LeaderAvailabilityTimers leaderAvailabilityTimers;

    public ConsensusModule( MemberId myself, final PlatformModule platformModule, Outbound<MemberId,RaftMessages.RaftMessage> outbound,
            File clusterStateDirectory, CoreTopologyService coreTopologyService, CoreStateStorageService coreStorageService, String activeDatabaseName )
    {
        final Config config = platformModule.config;
        final LogService logging = platformModule.logService;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;

        LogProvider logProvider = logging.getInternalLogProvider();

        Map<Integer,ChannelMarshal<ReplicatedContent>> marshals = new HashMap<>();
        marshals.put( 1, CoreReplicatedContentMarshalFactory.marshalV1( activeDatabaseName ) );
        marshals.put( 2, CoreReplicatedContentMarshalFactory.marshalV2() );

        RaftLog underlyingLog = createRaftLog( config, life, fileSystem, clusterStateDirectory, marshals, logProvider,
                platformModule.jobScheduler );

        raftLog = new MonitoredRaftLog( underlyingLog, platformModule.monitors );

        StateStorage<TermState> durableTermState = coreStorageService.stateStorage( RAFT_TERM );
        StateStorage<TermState> termState = new MonitoredTermStateStorage( durableTermState, platformModule.monitors );
        StateStorage<VoteState> voteState = coreStorageService.stateStorage( RAFT_VOTE );
        StateStorage<RaftMembershipState> raftMembershipStorage = coreStorageService.stateStorage( RAFT_MEMBERSHIP );

        TimerService timerService = new TimerService( platformModule.jobScheduler, logProvider );

        leaderAvailabilityTimers = createElectionTiming( config, timerService, logProvider );

        Integer minimumConsensusGroupSize = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_runtime );

        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();

        SendToMyself leaderOnlyReplicator = new SendToMyself( myself, outbound );

        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, memberSetBuilder, raftLog, logProvider,
                minimumConsensusGroupSize, leaderAvailabilityTimers.getElectionTimeout(), systemClock(), config.get( join_catch_up_timeout ).toMillis(),
                raftMembershipStorage );
        platformModule.dependencies.satisfyDependency( raftMembershipManager );

        life.add( raftMembershipManager );

        inFlightCache = InFlightCacheFactory.create( config, platformModule.monitors );

        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, timerService, systemClock(), myself,
                        raftMembershipManager, leaderAvailabilityTimers.getElectionTimeout(), config.get( catchup_batch_size ),
                        config.get( log_shipping_max_lag ), inFlightCache );

        boolean supportsPreVoting = config.get( CausalClusteringSettings.enable_pre_voting );

        raftMachine = new RaftMachine( myself, termState, voteState, raftLog, leaderAvailabilityTimers,
                outbound, logProvider, raftMembershipManager, logShipping, inFlightCache,
                config.get( refuse_to_be_leader ),
                supportsPreVoting, platformModule.monitors );

        DurationSinceLastMessageMonitor durationSinceLastMessageMonitor = new DurationSinceLastMessageMonitor();
        platformModule.monitors.addMonitorListener( durationSinceLastMessageMonitor );
        platformModule.dependencies.satisfyDependency( durationSinceLastMessageMonitor );

        String dbName = config.get( CausalClusteringSettings.database );

        life.add( new RaftCoreTopologyConnector( coreTopologyService, raftMachine, dbName ) );

        life.add( logShipping );
    }

    private LeaderAvailabilityTimers createElectionTiming( Config config, TimerService timerService, LogProvider logProvider )
    {
        Duration electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout );
        return new LeaderAvailabilityTimers( electionTimeout, electionTimeout.dividedBy( 3 ), systemClock(), timerService, logProvider );
    }

    private RaftLog createRaftLog( Config config, LifeSupport life, FileSystemAbstraction fileSystem, File clusterStateDirectory,
            Map<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector, LogProvider logProvider,
            JobScheduler scheduler )
    {
        EnterpriseCoreEditionModule.RaftLogImplementation raftLogImplementation =
                EnterpriseCoreEditionModule.RaftLogImplementation
                        .valueOf( config.get( CausalClusteringSettings.raft_log_implementation ) );
        switch ( raftLogImplementation )
        {
        case IN_MEMORY:
        {
            return new InMemoryRaftLog();
        }

        case SEGMENTED:
        {
            long rotateAtSize = config.get( CausalClusteringSettings.raft_log_rotation_size );
            int readerPoolSize = config.get( CausalClusteringSettings.raft_log_reader_pool_size );

            CoreLogPruningStrategy pruningStrategy =
                    new CoreLogPruningStrategyFactory( config.get( CausalClusteringSettings.raft_log_pruning_strategy ),
                            logProvider ).newInstance();
            File directory = CoreStateFiles.RAFT_LOG.at( clusterStateDirectory );

            return life.add( new SegmentedRaftLog( fileSystem, directory, rotateAtSize, marshalSelector::get, logProvider,
                    readerPoolSize, systemClock(), scheduler, pruningStrategy ) );
        }
        default:
            throw new IllegalStateException( "Unknown raft log implementation: " + raftLogImplementation );
        }
    }

    public RaftLog raftLog()
    {
        return raftLog;
    }

    public RaftMachine raftMachine()
    {
        return raftMachine;
    }

    public RaftMembershipManager raftMembershipManager()
    {
        return raftMembershipManager;
    }

    public InFlightCache inFlightCache()
    {
        return inFlightCache;
    }

    public LeaderAvailabilityTimers getLeaderAvailabilityTimers()
    {
        return leaderAvailabilityTimers;
    }
}
