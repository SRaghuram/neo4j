/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelInboundHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.causalclustering.catchup.v1.CatchupProtocolServerInstallerV1;
import org.neo4j.causalclustering.catchup.v2.CatchupProtocolServerInstallerV2;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public final class CatchupServerBuilder
{
    private CatchupServerBuilder()
    {
    }

    public static NeedsCatchupServerHandler builder()
    {
        return new StepBuilder();
    }

    private static class StepBuilder implements NeedsCatchupServerHandler, NeedsDefaultDatabaseName, NeedsCatchupProtocols, NeedsModifierProtocols,
            NeedsPipelineBuilder, NeedsInstalledProtocolsHandler, NeedsListenAddress, AcceptsOptionalParams
    {
        private CatchupServerHandler catchupServerHandler;
        private String defaultDatabaseName;
        private NettyPipelineBuilderFactory pipelineBuilder;
        private ApplicationSupportedProtocols catchupProtocols;
        private Collection<ModifierSupportedProtocols> modifierProtocols;
        private ChannelInboundHandler parentHandler;
        private ListenSocketAddress listenAddress;
        private LogProvider debugLogProvider = NullLogProvider.getInstance();
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private String serverName = "catchup-server";

        private StepBuilder()
        {
        }

        @Override
        public NeedsDefaultDatabaseName catchupServerHandler( CatchupServerHandler catchupServerHandler )
        {
            this.catchupServerHandler = catchupServerHandler;
            return this;
        }

        @Override
        public NeedsCatchupProtocols defaultDatabaseName( String defaultDatabaseName )
        {
            this.defaultDatabaseName = defaultDatabaseName;
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
        public AcceptsOptionalParams listenAddress( ListenSocketAddress listenAddress )
        {
            this.listenAddress = listenAddress;
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
        public Server build()
        {
            ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
            ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

            List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolInstallers = Arrays.asList(
                    new CatchupProtocolServerInstallerV1.Factory( pipelineBuilder, debugLogProvider, catchupServerHandler, defaultDatabaseName ),
                    new CatchupProtocolServerInstallerV2.Factory( pipelineBuilder, debugLogProvider, catchupServerHandler ) );

            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                    protocolInstallers, ModifierProtocolInstaller.allServerInstallers );

            HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilder, debugLogProvider );

            return new Server( handshakeServerInitializer, parentHandler, debugLogProvider, userLogProvider, listenAddress, serverName );
        }
    }

    public interface NeedsCatchupServerHandler
    {
        NeedsDefaultDatabaseName catchupServerHandler( CatchupServerHandler catchupServerHandler );
    }

    public interface NeedsDefaultDatabaseName
    {
        NeedsCatchupProtocols defaultDatabaseName( String defaultDatabaseName );
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
        AcceptsOptionalParams listenAddress( ListenSocketAddress listenAddress );
    }

    public interface AcceptsOptionalParams
    {
        AcceptsOptionalParams serverName( String serverName );
        AcceptsOptionalParams userLogProvider( LogProvider userLogProvider );
        AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider );
        Server build();
    }

}
