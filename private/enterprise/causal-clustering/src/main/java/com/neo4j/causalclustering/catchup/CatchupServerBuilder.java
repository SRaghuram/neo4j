/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstaller;
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
import io.netty.channel.socket.ServerSocketChannel;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

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
            NeedsPipelineBuilder, NeedsInstalledProtocolsHandler, NeedsListenAddress, NeedsScheduler, NeedsBootstrapConfig, NeedsPortRegister,
            AcceptsOptionalParams
    {
        private DatabaseIdRepository databaseIdRepository;
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
        public NeedsBootstrapConfig scheduler( JobScheduler scheduler )
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
        public AcceptsOptionalParams handshakeTimeout( Duration handshakeTimeout )
        {
            this.handshakeTimeout = handshakeTimeout;
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
                    applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
            ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

            List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolInstallers = List.of(
                    new CatchupProtocolServerInstaller.Factory( pipelineBuilder, debugLogProvider, catchupServerHandler ) );

            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                    protocolInstallers, ModifierProtocolInstaller.allServerInstallers );

            HandshakeServerInitializer handshakeInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilder, debugLogProvider );
            ServerChannelInitializer channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineBuilder, handshakeTimeout,
                    debugLogProvider );

            Executor executor = scheduler.executor( Group.CATCHUP_SERVER );

            return new Server( channelInitializer, parentHandler, debugLogProvider, userLogProvider, listenAddress, serverName, executor, portRegister,
                    bootstrapConfiguration );
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
        NeedsBootstrapConfig scheduler( JobScheduler scheduler );
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

        Server build();
    }
}
