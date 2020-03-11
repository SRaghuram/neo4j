/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import com.neo4j.causalclustering.messaging.LoggingInbound;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.init.ServerChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import io.netty.channel.ChannelInboundHandler;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.factory.module.GlobalModule;
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
    private final RaftMessageLogger<MemberId> raftMessageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;

    RaftServerFactory( GlobalModule globalModule, IdentityModule identityModule, NettyPipelineBuilderFactory pipelineBuilderFactory,
            RaftMessageLogger<MemberId> raftMessageLogger, ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols )
    {
        this.globalModule = globalModule;
        this.identityModule = identityModule;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.raftMessageLogger = raftMessageLogger;
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.supportedModifierProtocols = supportedModifierProtocols;
    }

    Server createRaftServer( RaftMessageDispatcher raftMessageDispatcher, ChannelInboundHandler installedProtocolsHandler )
    {
        Config config = globalModule.getGlobalConfig();

        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedApplicationProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

        RaftMessageNettyHandler nettyHandler = new RaftMessageNettyHandler( logProvider );
        RaftProtocolServerInstallerV2.Factory raftProtocolServerInstallerV2 =
                new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                new ProtocolInstallerRepository<>( List.of( raftProtocolServerInstallerV2 ),
                        ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilderFactory, logProvider, config );

        Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        ServerChannelInitializer channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineBuilderFactory,
                handshakeTimeout, logProvider, config );

        SocketAddress raftListenAddress = config.get( CausalClusteringSettings.raft_listen_address );

        Executor raftServerExecutor = globalModule.getJobScheduler().executor( Group.RAFT_SERVER );
        Server raftServer = new Server( channelInitializer, installedProtocolsHandler, logProvider,
                globalModule.getLogService().getUserLogProvider(), raftListenAddress, RAFT_SERVER_NAME, raftServerExecutor,
                globalModule.getConnectorPortRegister(), BootstrapConfiguration.serverConfig( config ) );

        LoggingInbound loggingRaftInbound = new LoggingInbound( nettyHandler, raftMessageLogger, identityModule.myself() );
        loggingRaftInbound.registerHandler( raftMessageDispatcher );

        return raftServer;
    }
}
