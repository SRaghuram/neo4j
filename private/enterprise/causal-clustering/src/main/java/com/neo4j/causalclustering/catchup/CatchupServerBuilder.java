/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolServerInstallerV4;
import com.neo4j.causalclustering.catchup.v5.CatchupProtocolServerInstallerV5;
import com.neo4j.causalclustering.core.ServerNameService;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.init.ServerChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.socket.ServerSocketChannel;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolUtil.buildServerInstallers;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolUtil.checkInstallersExhaustive;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_3_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_4_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_5_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.values;

public final class CatchupServerBuilder
{
    private CatchupServerBuilder()
    {
    }

    public static NeedsCatchupServerHandler builder()
    {
        return new StepBuilder();
    }

    private static class StepBuilder implements NeedsCatchupServerHandler, NeedsCatchupProtocols, NeedsModifierProtocols,
            NeedsPipelineBuilder, NeedsInstalledProtocolsHandler, NeedsListenAddress, NeedsScheduler,
            NeedsConfig, NeedsBootstrapConfig, NeedsPortRegister, AcceptsOptionalParams
    {
        private CatchupServerHandler catchupServerHandler;
        private NettyPipelineBuilderFactory pipelineBuilder;
        private ApplicationSupportedProtocols catchupProtocols;
        private Collection<ModifierSupportedProtocols> modifierProtocols;
        private ChannelInboundHandler parentHandler;
        private SocketAddress listenAddress;
        private JobScheduler scheduler;
        private LogProvider debugLogProvider = NullLogProvider.getInstance();
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private Duration handshakeTimeout = Duration.ofSeconds( 5 );
        private ConnectorPortRegister portRegister;
        private String serverName = "catchup-server";
        private BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration;
        private Config config = Config.defaults();
        private CatchupInboundEventListener catchupInboundEventListener = CatchupInboundEventListener.NO_OP;

        private StepBuilder()
        {
        }

        @Override
        public NeedsCatchupProtocols catchupServerHandler( CatchupServerHandler catchupServerHandler )
        {
            this.catchupServerHandler = catchupServerHandler;
            return this;
        }

        @Override
        public NeedsModifierProtocols catchupProtocols( ApplicationSupportedProtocols catchupProtocols )
        {
            this.catchupProtocols = catchupProtocols;
            return this;
        }

        @Override
        public NeedsPipelineBuilder modifierProtocols( Collection<ModifierSupportedProtocols> modifierProtocols )
        {
            this.modifierProtocols = modifierProtocols;
            return this;
        }

        @Override
        public NeedsInstalledProtocolsHandler pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder )
        {
            this.pipelineBuilder = pipelineBuilder;
            return this;
        }

        @Override
        public NeedsListenAddress installedProtocolsHandler( ChannelInboundHandler parentHandler )
        {
            this.parentHandler = parentHandler;
            return this;
        }

        @Override
        public NeedsScheduler listenAddress( SocketAddress listenAddress )
        {
            this.listenAddress = listenAddress;
            return this;
        }

        @Override
        public NeedsConfig scheduler( JobScheduler scheduler )
        {
            this.scheduler = scheduler;
            return this;
        }

        @Override
        public AcceptsOptionalParams portRegister( ConnectorPortRegister portRegister )
        {
            this.portRegister = portRegister;
            return this;
        }

        @Override
        public AcceptsOptionalParams serverName( String serverName )
        {
            this.serverName = serverName;
            return this;
        }

        @Override
        public AcceptsOptionalParams userLogProvider( LogProvider userLogProvider )
        {
            this.userLogProvider = userLogProvider;
            return this;
        }

        @Override
        public AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider )
        {
            this.debugLogProvider = debugLogProvider;
            return this;
        }

        @Override
        public AcceptsOptionalParams catchupInboundEventListener( CatchupInboundEventListener listener )
        {
            this.catchupInboundEventListener = listener;
            return this;
        }

        @Override
        public AcceptsOptionalParams handshakeTimeout( Duration handshakeTimeout )
        {
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        @Override
        public NeedsBootstrapConfig config( Config config )
        {
            this.config = config;
            return this;
        }

        @Override
        public NeedsPortRegister bootstrapConfig( BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration )
        {
            this.bootstrapConfiguration = bootstrapConfiguration;
            return this;
        }

        @Override
        public Server build()
        {
            ApplicationProtocolRepository
                    applicationProtocolRepository = new ApplicationProtocolRepository( values(), catchupProtocols );
            ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

            var serverLogService = new ServerNameService( debugLogProvider, userLogProvider, serverName );

            List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolInstallers =
                    buildProtocolList( serverLogService, config, pipelineBuilder, catchupServerHandler, catchupInboundEventListener );

            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                    protocolInstallers, ModifierProtocolInstaller.allServerInstallers );

            HandshakeServerInitializer handshakeInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilder, serverLogService.getInternalLogProvider(),
                    config );
            ServerChannelInitializer channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineBuilder, handshakeTimeout,
                    serverLogService.getInternalLogProvider(), config );

            Executor executor = scheduler.executor( Group.CATCHUP_SERVER );

            return new Server( channelInitializer, parentHandler, serverLogService, listenAddress,
                                      executor, portRegister, bootstrapConfiguration );
        }

        private static List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> buildProtocolList(
                ServerNameService serverNameService,
                Config config,
                NettyPipelineBuilderFactory pipelineBuilder,
                CatchupServerHandler catchupServerHandler,
                CatchupInboundEventListener catchupInboundEventListener )
        {
            final var maximumProtocol = ApplicationProtocols.maximumCatchupProtocol( config );
            final var protocolMap =
                    createApplicationProtocolMap( pipelineBuilder,
                                                  serverNameService.getInternalLogProvider(),
                                                  catchupServerHandler,
                                                  catchupInboundEventListener );
            checkInstallersExhaustive( protocolMap.keySet(), CATCHUP );

            return buildServerInstallers( protocolMap, maximumProtocol );
        }

        private static Map<ApplicationProtocol,ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> createApplicationProtocolMap(
                NettyPipelineBuilderFactory pipelineBuilder,
                LogProvider debugLogProvider,
                CatchupServerHandler catchupServerHandler,
                CatchupInboundEventListener listener )
        {
            return Map.of( CATCHUP_3_0, new CatchupProtocolServerInstallerV3.Factory( pipelineBuilder, debugLogProvider,
                                                                                      catchupServerHandler, listener ),
                           CATCHUP_4_0, new CatchupProtocolServerInstallerV4.Factory( pipelineBuilder, debugLogProvider,
                                                                                      catchupServerHandler, listener ),
                           CATCHUP_5_0, new CatchupProtocolServerInstallerV5.Factory( pipelineBuilder, debugLogProvider,
                                                                                      catchupServerHandler, listener ) );
        }
    }

    public interface NeedsCatchupServerHandler
    {
        NeedsCatchupProtocols catchupServerHandler( CatchupServerHandler catchupServerHandler );
    }

    public interface NeedsCatchupProtocols
    {
        NeedsModifierProtocols catchupProtocols( ApplicationSupportedProtocols catchupProtocols );
    }

    public interface NeedsModifierProtocols
    {
        NeedsPipelineBuilder modifierProtocols( Collection<ModifierSupportedProtocols> modifierProtocols );
    }

    public interface NeedsPipelineBuilder
    {
        NeedsInstalledProtocolsHandler pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder );
    }

    public interface NeedsInstalledProtocolsHandler
    {
        NeedsListenAddress installedProtocolsHandler( ChannelInboundHandler parentHandler );
    }

    public interface NeedsListenAddress
    {
        NeedsScheduler listenAddress( SocketAddress listenAddress );
    }

    public interface NeedsScheduler
    {
        NeedsConfig scheduler( JobScheduler scheduler );
    }

    public interface NeedsConfig
    {
        NeedsBootstrapConfig config( Config config );
    }

    public interface NeedsBootstrapConfig
    {
        NeedsPortRegister bootstrapConfig( BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration );
    }

    public interface NeedsPortRegister
    {
        AcceptsOptionalParams portRegister( ConnectorPortRegister portRegister );
    }

    public interface AcceptsOptionalParams
    {
        AcceptsOptionalParams serverName( String serverName );

        AcceptsOptionalParams userLogProvider( LogProvider userLogProvider );

        AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider );

        AcceptsOptionalParams handshakeTimeout( Duration handshakeTimeout );

        AcceptsOptionalParams catchupInboundEventListener( CatchupInboundEventListener listener );

        Server build();
    }
}
