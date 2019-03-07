/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.core.consensus.ConsensusModule;
import com.neo4j.causalclustering.core.consensus.ContinuousJob;
import com.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessageMonitoringHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessageTimerResetMonitor;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolServerInstallerV1;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.core.server.CoreServerModule;
import com.neo4j.causalclustering.core.state.RaftMessageApplier;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.MessageLogger;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.causalclustering.messaging.LoggingInbound;
import com.neo4j.causalclustering.messaging.RaftOutbound;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.RecoveryRequiredChecker;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.recovery.Recovery.recoveryRequiredChecker;

public class RaftServerModule
{
    public static final String RAFT_SERVER_NAME = "raft-server";

    private final GlobalModule globalModule;
    private final ConsensusModule consensusModule;
    private final IdentityModule identityModule;
    private final ApplicationSupportedProtocols supportedApplicationProtocol;
    private final MessageLogger<MemberId> messageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final RaftOutbound raftOutbound;

    private RaftServerModule( GlobalModule globalModule, ConsensusModule consensusModule, IdentityModule identityModule,
            CoreServerModule coreServerModule, NettyPipelineBuilderFactory pipelineBuilderFactory, MessageLogger<MemberId> messageLogger,
            CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider,
            ApplicationSupportedProtocols supportedApplicationProtocol, Collection<ModifierSupportedProtocols> supportedModifierProtocols,
            ChannelInboundHandler installedProtocolsHandler, String activeDatabaseName, Panicker panicker, RaftOutbound raftOutbound )
    {
        this.globalModule = globalModule;
        this.consensusModule = consensusModule;
        this.identityModule = identityModule;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.messageLogger = messageLogger;
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.catchupAddressProvider = catchupAddressProvider;
        this.supportedModifierProtocols = supportedModifierProtocols;
        this.raftOutbound = raftOutbound;
        LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>>
                messageHandlerChain = createMessageHandlerChain( coreServerModule, panicker );

        createRaftServer( coreServerModule, messageHandlerChain, installedProtocolsHandler, activeDatabaseName );
    }

    static void createAndStart( GlobalModule globalModule, ConsensusModule consensusModule, IdentityModule identityModule,
            CoreServerModule coreServerModule, NettyPipelineBuilderFactory pipelineBuilderFactory, MessageLogger<MemberId> messageLogger,
            CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider addressProvider, ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, ChannelInboundHandler installedProtocolsHandler,
            String activeDatabaseName, Panicker panicker, RaftOutbound raftOutbound )
    {
        new RaftServerModule( globalModule, consensusModule, identityModule, coreServerModule, pipelineBuilderFactory, messageLogger, addressProvider,
                supportedApplicationProtocol, supportedModifierProtocols, installedProtocolsHandler, activeDatabaseName, panicker, raftOutbound );
    }

    private void createRaftServer( CoreServerModule coreServerModule, LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandlerChain,
            ChannelInboundHandler installedProtocolsHandler, String activeDatabaseName )
    {
        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), supportedApplicationProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        RaftMessageNettyHandler nettyHandler = new RaftMessageNettyHandler( logProvider );
        RaftProtocolServerInstallerV2.Factory raftProtocolServerInstallerV2 =
                new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
        RaftProtocolServerInstallerV1.Factory raftProtocolServerInstallerV1 =
                new RaftProtocolServerInstallerV1.Factory( nettyHandler, pipelineBuilderFactory, activeDatabaseName,
                        logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                new ProtocolInstallerRepository<>( asList( raftProtocolServerInstallerV1, raftProtocolServerInstallerV2 ),
                        ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilderFactory, logProvider );

        Config globalConfig = globalModule.getGlobalConfig();
        ListenSocketAddress raftListenAddress = globalConfig.get( CausalClusteringSettings.raft_listen_address );

        Executor raftServerExecutor = globalModule.getJobScheduler().executor( Group.RAFT_SERVER );
        Server raftServer = new Server( handshakeServerInitializer, installedProtocolsHandler, logProvider,
                globalModule.getLogService().getUserLogProvider(), raftListenAddress, RAFT_SERVER_NAME, raftServerExecutor,
                BootstrapConfiguration.serverConfig( globalConfig ) );
        globalModule.getGlobalDependencies().satisfyDependency( raftServer ); // resolved in tests

        LoggingInbound<ReceivedInstantClusterIdAwareMessage<?>> loggingRaftInbound =
                new LoggingInbound<>( nettyHandler, messageLogger, identityModule.myself() );
        loggingRaftInbound.registerHandler( messageHandlerChain );

        RecoveryRequiredChecker recoveryChecker =
                recoveryRequiredChecker( globalModule.getFileSystem(), globalModule.getPageCache(), globalConfig, globalModule.getStorageEngineFactory() );

        //TODO: Understand that we add the CatchupServer to life here because we need to enforce an ordering between Raft and Catchup, but we should surface
        // all the separate components and do this ordered adding to life somewhere top level. Putting this in this method is just a bit weird.
        LifeSupport globalLife = globalModule.getGlobalLife();
        globalLife.add( raftServer ); // must start before core state so that it can trigger snapshot downloads when necessary
        globalLife.add( coreServerModule.createCoreLife( messageHandlerChain, logProvider, recoveryChecker ) );
        globalLife.add( coreServerModule.catchupServer() ); // must start last and stop first, since it handles external requests
        coreServerModule.backupServer().ifPresent( globalLife::add );
        globalLife.add( coreServerModule.downloadService() );
    }

    private LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> createMessageHandlerChain( CoreServerModule coreServerModule, Panicker panicker )
    {
        RaftMessageApplier messageApplier = new RaftMessageApplier( logProvider,
                consensusModule.raftMachine(), coreServerModule.downloadService(),
                coreServerModule.commandApplicationProcess(), catchupAddressProvider, panicker );

        Monitors globalMonitors = globalModule.getGlobalMonitors();
        ComposableMessageHandler monitoringHandler =
                RaftMessageMonitoringHandler.composable( globalModule.getGlobalClock(), globalMonitors );
        ComposableMessageHandler batchingMessageHandler = createBatchingHandler( globalModule.getGlobalConfig() );
        ComposableMessageHandler leaderAvailabilityHandler = LeaderAvailabilityHandler.composable( consensusModule.getLeaderAvailabilityTimers(),
                globalMonitors.newMonitor( RaftMessageTimerResetMonitor.class ), consensusModule.raftMachine()::term );
        ComposableMessageHandler clusterBindingHandler = ClusterBindingHandler.composable( logProvider );

        return clusterBindingHandler
                .compose( leaderAvailabilityHandler )
                .compose( batchingMessageHandler )
                .compose( monitoringHandler )
                .apply( messageApplier );
    }

    private ComposableMessageHandler createBatchingHandler( Config config )
    {
        Function<Runnable,ContinuousJob> jobFactory = runnable -> new ContinuousJob(
                globalModule.getJobScheduler().threadFactory( Group.RAFT_BATCH_HANDLER ), runnable,
                logProvider );

        BoundedPriorityQueue.Config inQueueConfig = new BoundedPriorityQueue.Config( config.get( CausalClusteringSettings.raft_in_queue_size ),
                config.get( CausalClusteringSettings.raft_in_queue_max_bytes ) );
        BatchingMessageHandler.Config batchConfig = new BatchingMessageHandler.Config(
                config.get( CausalClusteringSettings.raft_in_queue_max_batch ), config.get( CausalClusteringSettings.raft_in_queue_max_batch_bytes ) );

        return BatchingMessageHandler.composable( inQueueConfig, batchConfig, jobFactory, logProvider, raftOutbound );
    }
}
