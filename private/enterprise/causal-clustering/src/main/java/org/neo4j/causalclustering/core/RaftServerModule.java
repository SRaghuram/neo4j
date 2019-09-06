/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageMonitoringHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageTimerResetMonitor;
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolServerInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.RaftMessageApplier;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.messaging.LoggingInbound;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static java.util.Arrays.asList;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_in_queue_max_batch;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_in_queue_max_batch_bytes;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_in_queue_max_bytes;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_in_queue_size;

public class RaftServerModule
{
    public static final String RAFT_SERVER_NAME = "raft-server";

    private final PlatformModule platformModule;
    private final ConsensusModule consensusModule;
    private final MemberIdRepository memberIdRepository;
    private final ApplicationSupportedProtocols supportedApplicationProtocol;
    private final DatabaseService databaseService;
    private final MessageLogger<MemberId> messageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;

    private RaftServerModule( PlatformModule platformModule, ConsensusModule consensusModule, MemberIdRepository memberIdRepository,
            CoreServerModule coreServerModule, DatabaseService databaseService, NettyPipelineBuilderFactory pipelineBuilderFactory,
            MessageLogger<MemberId> messageLogger, CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider,
            ApplicationSupportedProtocols supportedApplicationProtocol, Collection<ModifierSupportedProtocols> supportedModifierProtocols,
            ChannelInboundHandler installedProtocolsHandler, String activeDatabaseName )
    {
        this.platformModule = platformModule;
        this.consensusModule = consensusModule;
        this.memberIdRepository = memberIdRepository;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.databaseService = databaseService;
        this.messageLogger = messageLogger;
        this.logProvider = platformModule.logging.getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.catchupAddressProvider = catchupAddressProvider;
        this.supportedModifierProtocols = supportedModifierProtocols;

        LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandlerChain = createMessageHandlerChain( coreServerModule );

        createRaftServer( coreServerModule, messageHandlerChain, installedProtocolsHandler, activeDatabaseName );
    }

    static void createAndStart( PlatformModule platformModule, ConsensusModule consensusModule, MemberIdRepository memberIdRepository,
            CoreServerModule coreServerModule, DatabaseService databaseService, NettyPipelineBuilderFactory pipelineBuilderFactory,
            MessageLogger<MemberId> messageLogger, CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider addressProvider,
            ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, ChannelInboundHandler installedProtocolsHandler,
            String activeDatabaseName )
    {
        new RaftServerModule( platformModule, consensusModule, memberIdRepository, coreServerModule, databaseService, pipelineBuilderFactory, messageLogger,
                        addressProvider, supportedApplicationProtocol, supportedModifierProtocols, installedProtocolsHandler, activeDatabaseName );
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

        ListenSocketAddress raftListenAddress = platformModule.config.get( CausalClusteringSettings.raft_listen_address );
        Server raftServer = new Server( handshakeServerInitializer, installedProtocolsHandler, logProvider, platformModule.logging.getUserLogProvider(),
                raftListenAddress, RAFT_SERVER_NAME );
        platformModule.dependencies.satisfyDependency( raftServer ); // resolved in tests

        LoggingInbound<ReceivedInstantClusterIdAwareMessage<?>> loggingRaftInbound =
                new LoggingInbound<>( nettyHandler, messageLogger, memberIdRepository.myself() );
        loggingRaftInbound.registerHandler( messageHandlerChain );

        RecoveryRequiredChecker recoveryChecker = new RecoveryRequiredChecker( platformModule.fileSystem, platformModule.pageCache,
                platformModule.config, platformModule.monitors );

        //TODO: Understand that we add the CatchupServer to life here because we need to enforce an ordering between Raft and Catchup, but we should surface
        // all the separate components and do this ordered adding to life somewhere top level. Putting this in this method is just a bit weird.
        platformModule.life.add( raftServer ); // must start before core state so that it can trigger snapshot downloads when necessary
        platformModule.life.add( coreServerModule.createCoreLife( messageHandlerChain, logProvider, recoveryChecker ) );
        platformModule.life.add( coreServerModule.catchupServer() ); // must start last and stop first, since it handles external requests
        coreServerModule.backupServer().ifPresent( platformModule.life::add );
        platformModule.life.add( coreServerModule.downloadService() );
    }

    private LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> createMessageHandlerChain( CoreServerModule coreServerModule )
    {
        RaftMessageApplier messageApplier = new RaftMessageApplier( databaseService, logProvider,
                consensusModule.raftMachine(), coreServerModule.downloadService(),
                coreServerModule.commandApplicationProcess(), catchupAddressProvider );

        ComposableMessageHandler monitoringHandler = RaftMessageMonitoringHandler.composable( platformModule.clock, platformModule.monitors );
        ComposableMessageHandler batchingMessageHandler = createBatchingHandler( platformModule.config );
        ComposableMessageHandler leaderAvailabilityHandler = LeaderAvailabilityHandler.composable( consensusModule.getLeaderAvailabilityTimers(),
                platformModule.monitors.newMonitor( RaftMessageTimerResetMonitor.class ), consensusModule.raftMachine()::term );
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
                platformModule.jobScheduler.threadFactory( Group.RAFT_BATCH_HANDLER ), runnable,
                logProvider );

        BoundedPriorityQueue.Config inQueueConfig = new BoundedPriorityQueue.Config( config.get( raft_in_queue_size ),
                config.get( raft_in_queue_max_bytes ) );
        BatchingMessageHandler.Config batchConfig = new BatchingMessageHandler.Config(
                config.get( raft_in_queue_max_batch ), config.get( raft_in_queue_max_batch_bytes ) );

        return BatchingMessageHandler.composable( inQueueConfig, batchConfig, jobFactory, logProvider );
    }
}
