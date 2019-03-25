/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.MessageLogger;
import com.neo4j.causalclustering.messaging.LoggingInbound;
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
import java.util.List;
import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

/**
 * Factory to create a global Raft server that listens to incoming messages
 * and forwards them to the appropriate handler chain via {@link RaftMessageDispatcher}.
 */
public class RaftServerFactory
{
    public static final String RAFT_SERVER_NAME = "raft-server";

    private final GlobalModule globalModule;
    private final IdentityModule identityModule;
    private final ApplicationSupportedProtocols supportedApplicationProtocol;
    private final MessageLogger<MemberId> messageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;

    public RaftServerFactory( GlobalModule globalModule, IdentityModule identityModule, NettyPipelineBuilderFactory pipelineBuilderFactory,
            MessageLogger<MemberId> messageLogger, ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols )
    {
        this.globalModule = globalModule;
        this.identityModule = identityModule;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.messageLogger = messageLogger;
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.supportedModifierProtocols = supportedModifierProtocols;
    }

    public Server createRaftServer( RaftMessageDispatcher raftMessageDispatcher, ChannelInboundHandler installedProtocolsHandler )
    {
        Config config = globalModule.getGlobalConfig();

        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), supportedApplicationProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        RaftMessageNettyHandler nettyHandler = new RaftMessageNettyHandler( logProvider );
        RaftProtocolServerInstallerV2.Factory raftProtocolServerInstallerV2 =
                new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                new ProtocolInstallerRepository<>( List.of( raftProtocolServerInstallerV2 ),
                        ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilderFactory, logProvider );

        ListenSocketAddress raftListenAddress = config.get( CausalClusteringSettings.raft_listen_address );

        Executor raftServerExecutor = globalModule.getJobScheduler().executor( Group.RAFT_SERVER );
        Server raftServer = new Server( handshakeServerInitializer, installedProtocolsHandler, logProvider,
                globalModule.getLogService().getUserLogProvider(), raftListenAddress, RAFT_SERVER_NAME, raftServerExecutor,
                BootstrapConfiguration.serverConfig( config ) );

        LoggingInbound<ReceivedInstantClusterIdAwareMessage<?>> loggingRaftInbound =
                new LoggingInbound<>( nettyHandler, messageLogger, identityModule.myself() );
        loggingRaftInbound.registerHandler( raftMessageDispatcher );

        return raftServer;
    }
}
