/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v3.RaftProtocolServerInstallerV3;
import com.neo4j.causalclustering.core.consensus.protocol.v4.RaftProtocolServerInstallerV4;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
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

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_2_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_3_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_4_0;
import static com.neo4j.configuration.CausalClusteringInternalSettings.experimental_raft_protocol;

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
    private final RaftMessageLogger<MemberId> raftMessageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final DatabaseIdRepository databaseIdRepository;

    RaftServerFactory( GlobalModule globalModule, ClusteringIdentityModule clusteringIdentityModule, NettyPipelineBuilderFactory pipelineBuilderFactory,
            RaftMessageLogger<MemberId> raftMessageLogger, ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, DatabaseIdRepository databaseIdRepository )
    {
        this.globalModule = globalModule;
        this.clusteringIdentityModule = clusteringIdentityModule;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.raftMessageLogger = raftMessageLogger;
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.supportedModifierProtocols = supportedModifierProtocols;
        this.databaseIdRepository = databaseIdRepository;
    }

    Server createRaftServer( RaftMessageDispatcher raftMessageDispatcher, ChannelInboundHandler installedProtocolsHandler )
    {
        var config = globalModule.getGlobalConfig();

        var applicationProtocolRepository =
                new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedApplicationProtocol );
        var modifierProtocolRepository =
                new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

        var nettyHandler = new RaftMessageNettyHandler( logProvider );

        var maximumRaftVersion = globalModule.getGlobalConfig().get( experimental_raft_protocol ) ? ApplicationProtocols.RAFT_4_0
                                                                                                  : ApplicationProtocols.RAFT_3_0;
        var protocolInstallerRepository = new ProtocolInstallerRepository<>(
                createProtocolList( nettyHandler, maximumRaftVersion ),
                ModifierProtocolInstaller.allServerInstallers );

        var handshakeInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                                                                   protocolInstallerRepository, pipelineBuilderFactory, logProvider, config );

        var handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        var channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineBuilderFactory,
                                                               handshakeTimeout, logProvider, config );

        var raftListenAddress = config.get( CausalClusteringSettings.raft_listen_address );

        var raftServerExecutor = globalModule.getJobScheduler().executor( Group.RAFT_SERVER );
        var raftServer = new Server( channelInitializer, installedProtocolsHandler, logProvider,
                                     globalModule.getLogService().getUserLogProvider(), raftListenAddress, RAFT_SERVER_NAME, raftServerExecutor,
                                     globalModule.getConnectorPortRegister(), BootstrapConfiguration.serverConfig( config ) );

        var loggingRaftInbound = new LoggingInbound( nettyHandler, raftMessageLogger, clusteringIdentityModule.memberId(), databaseIdRepository );
        loggingRaftInbound.registerHandler( raftMessageDispatcher );

        return raftServer;
    }

    private List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> createProtocolList( RaftMessageNettyHandler nettyHandler,
                                                                                                        ApplicationProtocols maximumProtocol )
    {
        return createApplicationProtocolMap( nettyHandler )
                .entrySet()
                .stream()
                .filter( p -> p.getKey().lessOrEquals( maximumProtocol ) )
                .map( Map.Entry::getValue )
                .collect( Collectors.toList() );
    }

    private Map<ApplicationProtocols,ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> createApplicationProtocolMap(
            RaftMessageNettyHandler nettyHandler )
    {
        return Map.of( RAFT_2_0, new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider ),
                       RAFT_3_0, new RaftProtocolServerInstallerV3.Factory( nettyHandler, pipelineBuilderFactory, logProvider ),
                       RAFT_4_0,
                       new RaftProtocolServerInstallerV4.Factory( nettyHandler, pipelineBuilderFactory, logProvider, globalModule.getGlobalClock() ) );
    }
}
