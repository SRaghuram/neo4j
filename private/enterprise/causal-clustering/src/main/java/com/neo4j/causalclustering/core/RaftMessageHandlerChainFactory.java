/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider;
import com.neo4j.causalclustering.core.consensus.RaftGroup;
import com.neo4j.causalclustering.core.consensus.ContinuousJob;
import com.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessageMonitoringHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessageTimerResetMonitor;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.core.server.CoreServerModule;
import com.neo4j.causalclustering.core.state.RaftMessageApplier;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

/**
 * Factory to create a chain of {@link LifecycleMessageHandler handlers}.
 * This factory is constructed using global components and can be then used to create a chain of database-specific handlers.
 */
public class RaftMessageHandlerChainFactory
{
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final Panicker panicker;
    private final JobScheduler jobScheduler;
    private final Clock clock;
    private final LogProvider logProvider;
    private final Monitors monitors;
    private final Config config;

    public RaftMessageHandlerChainFactory( GlobalModule globalModule, RaftMessageDispatcher raftMessageDispatcher,
            LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider, Panicker panicker )
    {
        this.raftMessageDispatcher = raftMessageDispatcher;
        this.catchupAddressProvider = catchupAddressProvider;
        this.panicker = panicker;
        this.jobScheduler = globalModule.getJobScheduler();
        this.clock = globalModule.getGlobalClock();
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.monitors = globalModule.getGlobalMonitors();
        this.config = globalModule.getGlobalConfig();
    }

    public LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> createMessageHandlerChain( RaftGroup raftGroup,
            CoreServerModule coreServerModule )
    {
        RaftMessageApplier messageApplier = new RaftMessageApplier( logProvider,
                raftGroup.raftMachine(), coreServerModule.downloadService(),
                coreServerModule.commandApplicationProcess(), catchupAddressProvider, panicker );

        ComposableMessageHandler monitoringHandler = RaftMessageMonitoringHandler.composable( clock, monitors );
        ComposableMessageHandler batchingMessageHandler = createBatchingHandler();
        ComposableMessageHandler leaderAvailabilityHandler = LeaderAvailabilityHandler.composable( raftGroup.getLeaderAvailabilityTimers(),
                monitors.newMonitor( RaftMessageTimerResetMonitor.class ), raftGroup.raftMachine()::term );
        ComposableMessageHandler clusterBindingHandler = ClusterBindingHandler.composable( raftMessageDispatcher, logProvider );

        return clusterBindingHandler
                .compose( leaderAvailabilityHandler )
                .compose( batchingMessageHandler )
                .compose( monitoringHandler )
                .apply( messageApplier );
    }

    private ComposableMessageHandler createBatchingHandler()
    {
        BoundedPriorityQueue.Config inQueueConfig = new BoundedPriorityQueue.Config( config.get( CausalClusteringSettings.raft_in_queue_size ),
                config.get( CausalClusteringSettings.raft_in_queue_max_bytes ) );
        BatchingMessageHandler.Config batchConfig = new BatchingMessageHandler.Config(
                config.get( CausalClusteringSettings.raft_in_queue_max_batch ), config.get( CausalClusteringSettings.raft_in_queue_max_batch_bytes ) );

        return BatchingMessageHandler.composable( inQueueConfig, batchConfig, this::jobFactory, logProvider );
    }

    private ContinuousJob jobFactory( Runnable runnable )
    {
        return new ContinuousJob( jobScheduler.threadFactory( Group.RAFT_BATCH_HANDLER ), runnable, logProvider );
    }
}
