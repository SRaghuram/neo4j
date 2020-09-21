/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolServerInstaller;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import com.neo4j.causalclustering.messaging.LoggingInbound;
import com.neo4j.causalclustering.messaging.marshalling.DecodingDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageComposer;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.v3.decoding.RaftMessageDecoderV3;
import com.neo4j.causalclustering.messaging.marshalling.v4.decoding.RaftMessageDecoderV4;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.init.ServerChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import com.neo4j.configuration.CausalClusteringSettings;
import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolUtil.buildServerInstallers;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolUtil.checkInstallersExhaustive;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_2_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_3_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_4_0;
import static com.neo4j.configuration.CausalClusteringInternalSettings.experimental_raft_protocol;
import static java.util.function.UnaryOperator.identity;

/**
 * Factory to create a global Raft server that listens to incoming messages and forwards them to the appropriate handler chain via {@link
 * RaftMessageDispatcher}.
 */
public class RaftServerFactory
{
    public static final String RAFT_SERVER_NAME = "raft-server";

    private final GlobalModule globalModule;
    private final ClusteringIdentityModule clusteringIdentityModule;
    private final ApplicationSupportedProtocols supportedApplicationProtocol;
    private final RaftMessageLogger<RaftMemberId> raftMessageLogger;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final DatabaseIdRepository databaseIdRepository;
    private final ServerLogService serverLogService;
    private final LogProvider internalLogProvider;

    RaftServerFactory( GlobalModule globalModule, ClusteringIdentityModule clusteringIdentityModule, NettyPipelineBuilderFactory pipelineBuilderFactory,
            RaftMessageLogger<RaftMemberId> raftMessageLogger, ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, DatabaseIdRepository databaseIdRepository )
    {
        this.globalModule = globalModule;
        this.clusteringIdentityModule = clusteringIdentityModule;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.raftMessageLogger = raftMessageLogger;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.supportedModifierProtocols = supportedModifierProtocols;
        this.databaseIdRepository = databaseIdRepository;
        this.serverLogService = new ServerLogService( globalModule.getLogService(), RAFT_SERVER_NAME );
        this.internalLogProvider = serverLogService.getInternalLogProvider();
    }

    Server createRaftServer( RaftMessageDispatcher raftMessageDispatcher, ChannelInboundHandler installedProtocolsHandler )
    {
        var config = globalModule.getGlobalConfig();

        var applicationProtocolRepository =
                new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedApplicationProtocol );
        var modifierProtocolRepository =
                new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

        var nettyHandler = new RaftMessageNettyHandler( internalLogProvider );

        var maximumRaftVersion = globalModule.getGlobalConfig().get( experimental_raft_protocol ) ? ApplicationProtocols.RAFT_4_0
                                                                                                  : ApplicationProtocols.RAFT_3_0;
        var protocolInstallerRepository = new ProtocolInstallerRepository<>(
                createProtocolList( nettyHandler, maximumRaftVersion ),
                ModifierProtocolInstaller.allServerInstallers );

        var handshakeInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilderFactory, internalLogProvider, config );

        var handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        var channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineBuilderFactory,
                handshakeTimeout, internalLogProvider, config );

        var raftListenAddress = config.get( CausalClusteringSettings.raft_listen_address );

        var raftServerExecutor = globalModule.getJobScheduler().executor( Group.RAFT_SERVER );
        var raftServer = new Server( channelInitializer, installedProtocolsHandler, serverLogService, raftListenAddress, RAFT_SERVER_NAME, raftServerExecutor,
                globalModule.getConnectorPortRegister(), BootstrapConfiguration.serverConfig( config ) );

        var myself = /*RaftMessageLogger*/ RaftMemberId.from( clusteringIdentityModule.memberId() );
        var loggingRaftInbound = new LoggingInbound( nettyHandler, raftMessageLogger, myself, databaseIdRepository );
        loggingRaftInbound.registerHandler( raftMessageDispatcher );

        return raftServer;
    }

    private List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> createProtocolList( RaftMessageNettyHandler nettyHandler,
                                                                                                        ApplicationProtocols maximumProtocol )
    {
        final var protocolMap = createApplicationProtocolMap( nettyHandler );
        checkInstallersExhaustive( protocolMap.keySet(), ApplicationProtocolCategory.RAFT );

        return buildServerInstallers( protocolMap, maximumProtocol );
    }

    private Map<ApplicationProtocol,ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> createApplicationProtocolMap(
            RaftMessageNettyHandler nettyHandler )
    {
        final var clock = globalModule.getGlobalClock();
        final var factoryV2 = new RaftProtocolServerInstaller.Factory( nettyHandler,
                pipelineBuilderFactory,
                internalLogProvider,
                c -> new DecodingDispatcher( c, internalLogProvider, RaftMessageDecoder::new ),
                () -> new RaftMessageComposer( clock ),
                RAFT_2_0 );
        final var factoryV3 = new RaftProtocolServerInstaller.Factory( nettyHandler,
                pipelineBuilderFactory,
                internalLogProvider,
                c -> new DecodingDispatcher( c, internalLogProvider, RaftMessageDecoderV3::new ),
                () -> new RaftMessageComposer( clock ),
                RAFT_3_0 );
        final var factoryV4 = new RaftProtocolServerInstaller.Factory( nettyHandler,
                pipelineBuilderFactory,
                internalLogProvider,
                c -> new DecodingDispatcher( c, internalLogProvider, RaftMessageDecoderV4::new ),
                () -> new RaftMessageComposer( clock ),
                RAFT_4_0 );

        return List.of( factoryV2, factoryV3, factoryV4 )
                   .stream()
                   .collect( Collectors.toMap( ProtocolInstaller.Factory::applicationProtocol, identity() ) );
    }
}
