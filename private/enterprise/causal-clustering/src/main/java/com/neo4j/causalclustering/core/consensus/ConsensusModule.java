/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.common.RaftLogImplementation;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.MonitoredRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheFactory;
import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategy;
import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import com.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSetBuilder;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.term.MonitoredTermStateStorage;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.SendToMyself;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.RaftCoreTopologyConnector;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.common.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.catchup_batch_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.join_catch_up_timeout;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.log_shipping_max_lag;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static org.neo4j.time.Clocks.systemClock;

public class ConsensusModule
{
    private final MonitoredRaftLog raftLog;
    private final RaftMachine raftMachine;
    private final RaftMembershipManager raftMembershipManager;
    private final InFlightCache inFlightCache;

    private final LeaderAvailabilityTimers leaderAvailabilityTimers;

    public ConsensusModule( MemberId myself, final GlobalModule globalModule, Outbound<MemberId,RaftMessages.RaftMessage> outbound,
            ClusterStateLayout layout, CoreTopologyService coreTopologyService, CoreStateStorageFactory storageFactory,
            String defaultDatabaseName )
    {
        final Config globalConfig = globalModule.getGlobalConfig();
        final LogService logService = globalModule.getLogService();
        final FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        final LifeSupport globalLife = globalModule.getGlobalLife();
        final Monitors globalMonitors = globalModule.getGlobalMonitors();
        final Dependencies globalDependencies = globalModule.getGlobalDependencies();

        LogProvider logProvider = logService.getInternalLogProvider();

        Map<Integer,ChannelMarshal<ReplicatedContent>> marshals = new HashMap<>();
        marshals.put( 1, CoreReplicatedContentMarshalFactory.marshalV1( defaultDatabaseName ) );
        marshals.put( 2, CoreReplicatedContentMarshalFactory.marshalV2() );

        JobScheduler jobScheduler = globalModule.getJobScheduler();
        RaftLog underlyingLog = createRaftLog( globalConfig, globalLife, fileSystem, layout, marshals, logProvider, jobScheduler, defaultDatabaseName );

        raftLog = new MonitoredRaftLog( underlyingLog, globalMonitors );

        StateStorage<TermState> durableTermState = storageFactory.createRaftTermStorage( defaultDatabaseName, globalLife );
        StateStorage<TermState> termState = new MonitoredTermStateStorage( durableTermState, globalMonitors );
        StateStorage<VoteState> voteState = storageFactory.createRaftVoteStorage( defaultDatabaseName, globalLife );
        StateStorage<RaftMembershipState> raftMembershipStorage = storageFactory.createRaftMembershipStorage( defaultDatabaseName, globalLife );

        TimerService timerService = new TimerService( jobScheduler, logProvider );

        leaderAvailabilityTimers = createElectionTiming( globalConfig, timerService, logProvider );

        Integer minimumConsensusGroupSize = globalConfig.get( CausalClusteringSettings.minimum_core_cluster_size_at_runtime );

        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();

        SendToMyself leaderOnlyReplicator = new SendToMyself( myself, outbound );

        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, memberSetBuilder, raftLog, logProvider,
                minimumConsensusGroupSize, leaderAvailabilityTimers.getElectionTimeout(), systemClock(), globalConfig.get( join_catch_up_timeout ).toMillis(),
                raftMembershipStorage );
        globalDependencies.satisfyDependency( raftMembershipManager );

        globalLife.add( raftMembershipManager );

        inFlightCache = InFlightCacheFactory.create( globalConfig, globalMonitors );

        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, timerService, systemClock(), myself,
                        raftMembershipManager, leaderAvailabilityTimers.getElectionTimeout(), globalConfig.get( catchup_batch_size ),
                        globalConfig.get( log_shipping_max_lag ), inFlightCache );

        boolean supportsPreVoting = globalConfig.get( CausalClusteringSettings.enable_pre_voting );

        raftMachine = new RaftMachine( myself, termState, voteState, raftLog, leaderAvailabilityTimers,
                outbound, logProvider, raftMembershipManager, logShipping, inFlightCache,
                globalConfig.get( refuse_to_be_leader ),
                supportsPreVoting, globalMonitors );

        DurationSinceLastMessageMonitor durationSinceLastMessageMonitor = new DurationSinceLastMessageMonitor( globalModule.getGlobalClock() );
        globalMonitors.addMonitorListener( durationSinceLastMessageMonitor );
        globalDependencies.satisfyDependency( durationSinceLastMessageMonitor );

        String dbName = globalConfig.get( CausalClusteringSettings.database );

        globalLife.add( new RaftCoreTopologyConnector( coreTopologyService, raftMachine, dbName ) );

        globalLife.add( logShipping );
    }

    private LeaderAvailabilityTimers createElectionTiming( Config config, TimerService timerService, LogProvider logProvider )
    {
        Duration electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout );
        return new LeaderAvailabilityTimers( electionTimeout, electionTimeout.dividedBy( 3 ), systemClock(), timerService, logProvider );
    }

    private RaftLog createRaftLog( Config config, LifeSupport life, FileSystemAbstraction fileSystem, ClusterStateLayout layout,
            Map<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector, LogProvider logProvider,
            JobScheduler scheduler, String defaultDatabaseName )
    {
        RaftLogImplementation raftLogImplementation =
                RaftLogImplementation
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

            // Default database name is used here temporarily because both system and default database
            // live in a single Raft group and append to the same Raft log under `cluster-state/db/neo4j/raft-log` directory.
            // This will change once we have multiple Raft logs, then the correct database name will be used here.
            // E.g. system will use "system" to append to a Raft log located under `cluster-state/db/system/raft-log`
            File directory = layout.raftLogDirectory( defaultDatabaseName );

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
