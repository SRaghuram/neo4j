/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.common.RaftLogImplementation;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.leader_transfer.ExpiringSet;
import com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferService;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.MonitoredRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategy;
import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import com.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSetBuilder;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContext;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.core.consensus.term.MonitoredTermStateStorage;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.SendToMyself;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshal;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.InFlightCacheFactory;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.state.StateStorage;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.configuration.CausalClusteringSettings.catchup_batch_size;
import static com.neo4j.configuration.CausalClusteringSettings.join_catch_up_max_lag;
import static com.neo4j.configuration.CausalClusteringSettings.join_catch_up_timeout;
import static com.neo4j.configuration.CausalClusteringSettings.log_shipping_max_lag;
import static com.neo4j.configuration.CausalClusteringSettings.log_shipping_retry_timeout;

public class RaftGroup
{
    private final MonitoredRaftLog raftLog;
    private final RaftMachine raftMachine;
    private final RaftMembershipManager raftMembershipManager;
    private final InFlightCache inFlightCache;
    private final LeaderAvailabilityTimers leaderAvailabilityTimers;

    RaftGroup( Config config, DatabaseLogService logService, FileSystemAbstraction fileSystem, JobScheduler jobScheduler, SystemNanoClock clock,
            Lazy<RaftMemberId> myself, LifeSupport raftComponents, Monitors monitors, Dependencies dependencies,
            Outbound<RaftMemberId,RaftMessages.RaftMessage> outbound, ClusterStateLayout clusterState,
            ClusterStateStorageFactory storageFactory, NamedDatabaseId namedDatabaseId, LeaderTransferService leaderTransferService,
            LeaderListener leaderListener, MemoryTracker memoryTracker, ServerGroupsSupplier serverGroupsSupplier, AvailabilityGuard globalAvailabilityGuard,
            Consumer<RaftMessages.StatusResponse> statusResponseConsumer, LogEntryWriterFactory logEntryWriterFactory )
    {
        DatabaseLogProvider logProvider = logService.getInternalLogProvider();
        TimerService timerService = new TimerService( jobScheduler, logProvider );

        Map<Integer,ChannelMarshal<ReplicatedContent>> marshals = Map.of( 2, new CoreReplicatedContentMarshal( logEntryWriterFactory ) );
        RaftLog underlyingLog =
                createRaftLog( config, raftComponents, fileSystem, clusterState, marshals, logProvider, jobScheduler, namedDatabaseId, memoryTracker, clock );
        raftLog = new MonitoredRaftLog( underlyingLog, monitors );

        StateStorage<TermState> durableTermState = storageFactory.createRaftTermStorage( namedDatabaseId.name(), raftComponents, logProvider );
        StateStorage<TermState> termState = new MonitoredTermStateStorage( durableTermState, monitors );
        StateStorage<VoteState> voteState = storageFactory.createRaftVoteStorage( namedDatabaseId.name(), raftComponents, logProvider );
        StateStorage<RaftMembershipState> raftMembershipStorage = storageFactory.createRaftMembershipStorage(
                namedDatabaseId.name(), raftComponents, logProvider );

        var raftTimersConfig = new RaftTimersConfig( config );
        leaderAvailabilityTimers = new LeaderAvailabilityTimers( raftTimersConfig, clock, timerService, logProvider );

        SendToMyself leaderOnlyReplicator = new SendToMyself( myself, outbound );
        Integer minimumConsensusGroupSize = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_runtime );
        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();
        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, myself, memberSetBuilder, raftLog, logProvider, minimumConsensusGroupSize,
                                                           config.get( join_catch_up_max_lag ).toMillis(), clock,
                                                           config.get( join_catch_up_timeout ).toMillis(), raftMembershipStorage );

        dependencies.satisfyDependency( raftMembershipManager );
        raftComponents.add( raftMembershipManager );

        // TODO: In-flight cache should support sharing between multiple databases.
        inFlightCache = InFlightCacheFactory.create( config, monitors );
        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, timerService, clock, myself, raftMembershipManager,
                                            config.get( log_shipping_retry_timeout ).toMillis(), config.get( catchup_batch_size ),
                                            config.get( log_shipping_max_lag ),
                                            inFlightCache );

        var leaderTransfers = new ExpiringSet<RaftMemberId>( config.get( CausalClusteringInternalSettings.leader_transfer_timeout ), clock );

        var state = new RaftState( myself, termState, raftMembershipManager, raftLog, voteState, inFlightCache, logProvider, leaderTransfers );
        var messageHandlingContext = new RaftMessageHandlingContext( state, config, serverGroupsSupplier, globalAvailabilityGuard::isShutdown );

        var raftMessageTimerResetMonitor = monitors.newMonitor( RaftMessageTimerResetMonitor.class );
        var raftOutcomeApplier = new RaftOutcomeApplier( state, outbound, leaderAvailabilityTimers, raftMessageTimerResetMonitor, logShipping,
                                                         raftMembershipManager, logProvider,
                                                         rejection -> leaderTransferService.handleRejection( rejection, namedDatabaseId ),
                                                         statusResponseConsumer );

        raftMachine = new RaftMachine( myself, leaderAvailabilityTimers, logProvider, raftMembershipManager, inFlightCache, raftOutcomeApplier,
                                       state, messageHandlingContext );

        raftMachine.registerListener( leaderListener );

        DurationSinceLastMessageMonitor durationSinceLastMessageMonitor = new DurationSinceLastMessageMonitor( clock );
        monitors.addMonitorListener( durationSinceLastMessageMonitor );
        dependencies.satisfyDependency( durationSinceLastMessageMonitor );

        raftComponents.add( logShipping );
    }

    private static RaftLog createRaftLog( Config config, LifeSupport life, FileSystemAbstraction fileSystem, ClusterStateLayout layout,
                                          Map<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector, LogProvider logProvider, JobScheduler scheduler,
                                          NamedDatabaseId namedDatabaseId, MemoryTracker memoryTracker, SystemNanoClock clock )
    {
        RaftLogImplementation raftLogImplementation = RaftLogImplementation.valueOf( config.get( CausalClusteringSettings.raft_log_implementation ) );
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

            CoreLogPruningStrategy pruningStrategy = new CoreLogPruningStrategyFactory(
                    config.get( CausalClusteringSettings.raft_log_pruning_strategy ), logProvider ).newInstance();

            Path directory = layout.raftLogDirectory( namedDatabaseId.name() );

            return life.add(
                    new SegmentedRaftLog( fileSystem, directory, rotateAtSize, marshalSelector::get, logProvider, readerPoolSize, clock, scheduler,
                                          pruningStrategy, memoryTracker ) );
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
