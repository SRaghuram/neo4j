/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider;
import com.neo4j.causalclustering.core.batching.BatchingMessageHandler;
import com.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import com.neo4j.causalclustering.core.consensus.RaftGroup;
import com.neo4j.causalclustering.core.consensus.RaftMessageMonitoringHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessageTimerResetMonitor;
import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.RaftMessageApplier;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

/**
 * Factory to create a chain of {@link LifecycleMessageHandler handlers}.
 * This factory is constructed using global components and can be then used to create a chain of database-specific handlers.
 */
class RaftMessageHandlerChainFactory
{
    private final RaftMessageDispatcher raftMessageDispatcher;
    private final LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final DatabasePanicker panicker;
    private final JobScheduler jobScheduler;
    private final Clock clock;
    private final LogProvider logProvider;
    private final Monitors monitors;
    private final Config config;

    RaftMessageHandlerChainFactory( JobScheduler jobScheduler, Clock clock, LogProvider logProvider, Monitors monitors, Config config,
            RaftMessageDispatcher raftMessageDispatcher, LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider, DatabasePanicker panicker )
    {
        this.jobScheduler = jobScheduler;
        this.clock = clock;
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.config = config;

        this.raftMessageDispatcher = raftMessageDispatcher;
        this.catchupAddressProvider = catchupAddressProvider;
        this.panicker = panicker;
    }

    LifecycleMessageHandler<InboundRaftMessageContainer<?>> createMessageHandlerChain( RaftGroup raftGroup, CoreDownloaderService downloaderService,
            CommandApplicationProcess commandApplicationProcess )
    {
        RaftMessageApplier messageApplier = new RaftMessageApplier( logProvider, raftGroup.raftMachine(), downloaderService, commandApplicationProcess,
                                                                    catchupAddressProvider, panicker );

        ComposableMessageHandler monitoringHandler = RaftMessageMonitoringHandler.composable( clock, monitors );
        ComposableMessageHandler batchingMessageHandler = BatchingMessageHandler.composable( config, jobScheduler, logProvider );
        ComposableMessageHandler leaderAvailabilityHandler = LeaderAvailabilityHandler.composable( raftGroup.getLeaderAvailabilityTimers(),
                                                                                                   monitors.newMonitor( RaftMessageTimerResetMonitor.class ),
                                                                                                   raftGroup.raftMachine()::term );
        ComposableMessageHandler clusterBindingHandler = ClusterBindingHandler.composable( raftMessageDispatcher, logProvider );

        return clusterBindingHandler
                .compose( leaderAvailabilityHandler )
                .compose( batchingMessageHandler )
                .compose( monitoringHandler )
                .apply( messageApplier );
    }
}
