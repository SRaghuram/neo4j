/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolClientInstallerV4;
import com.neo4j.causalclustering.catchup.v5.CatchupProtocolClientInstallerV5;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.init.ClientChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import io.netty.channel.socket.SocketChannel;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolUtil.buildClientInstallers;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolUtil.checkInstallersExhaustive;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_3_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_4_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_5_0;

public final class CatchupClientBuilder
{
    private CatchupClientBuilder()
    {
    }

    public static NeedsCatchupProtocols builder()
    {
        return new StepBuilder();
    }

    private static class StepBuilder implements NeedsCatchupProtocols, NeedsModifierProtocols, NeedsPipelineBuilder,
                                                NeedsInactivityTimeout, NeedsScheduler, NeedBootstrapConfig, NeedCommandReader, AcceptsOptionalParams,
                                                NeedsConfig
    {
        private NettyPipelineBuilderFactory pipelineBuilder;
        private ApplicationSupportedProtocols catchupProtocols;
        private Collection<ModifierSupportedProtocols> modifierProtocols;
        private JobScheduler scheduler;
        private LogProvider debugLogProvider = NullLogProvider.getInstance();
        private Duration inactivityTimeout;
        private Duration handshakeTimeout = Duration.ofSeconds( 5 );
        private Clock clock;
        private BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration;
        private CommandReaderFactory commandReaderFactory;
        private Config config;

        private StepBuilder()
        {
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
        public NeedsInactivityTimeout pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder )
        {
            this.pipelineBuilder = pipelineBuilder;
            return this;
        }

        @Override
        public NeedsScheduler inactivityTimeout( Duration inactivityTimeout )
        {
            this.inactivityTimeout = inactivityTimeout;
            return this;
        }

        @Override
        public NeedsConfig scheduler( JobScheduler scheduler )
        {
            this.scheduler = scheduler;
            return this;
        }

        @Override
        public AcceptsOptionalParams handShakeTimeout( Duration handshakeTimeout )
        {
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        @Override
        public AcceptsOptionalParams clock( Clock clock )
        {
            this.clock = clock;
            return this;
        }

        @Override
        public AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider )
        {
            this.debugLogProvider = debugLogProvider;
            return this;
        }

        @Override
        public NeedCommandReader bootstrapConfig( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration )
        {
            this.bootstrapConfiguration = bootstrapConfiguration;
            return this;
        }

        @Override
        public AcceptsOptionalParams commandReader( CommandReaderFactory commandReaderFactory )
        {
            this.commandReaderFactory = commandReaderFactory;
            return this;
        }

        @Override
        public NeedBootstrapConfig config( Config config )
        {
            this.config = config;
            return this;
        }

        @Override
        public CatchupClientFactory build()
        {
            ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
            ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

            Function<CatchupResponseHandler,ClientChannelInitializer> channelInitializerFactory = handler ->
            {
                List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Client,?>> installers = buildProtocolList( handler );

                ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>( installers,
                        ModifierProtocolInstaller.allClientInstallers );

                HandshakeClientInitializer handshakeInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                                                                                                  protocolInstallerRepository, pipelineBuilder,
                                                                                                  handshakeTimeout, debugLogProvider, debugLogProvider );

                return new ClientChannelInitializer( handshakeInitializer, pipelineBuilder, handshakeTimeout, debugLogProvider );
            };

            CatchupChannelPoolService catchupChannelPoolService = new CatchupChannelPoolService(
                    bootstrapConfiguration, scheduler, clock, channelInitializerFactory );

            return new CatchupClientFactory( inactivityTimeout, catchupChannelPoolService );
        }

        private List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Client,?>> buildProtocolList( CatchupResponseHandler handler )
        {
            final var maximumProtocol = ApplicationProtocols.maximumCatchupProtocol( config );
            final var protocolMap = buildProtocolMap( handler );

            checkInstallersExhaustive( protocolMap.keySet(), CATCHUP );

            return buildClientInstallers( protocolMap, maximumProtocol );
        }

        private Map<ApplicationProtocol,ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Client,?>> buildProtocolMap( CatchupResponseHandler handler )
        {
            return Map.of( CATCHUP_3_0, new CatchupProtocolClientInstallerV3.Factory( pipelineBuilder, debugLogProvider, handler, commandReaderFactory ),
                           CATCHUP_4_0, new CatchupProtocolClientInstallerV4.Factory( pipelineBuilder, debugLogProvider, handler, commandReaderFactory ),
                           CATCHUP_5_0, new CatchupProtocolClientInstallerV5.Factory( pipelineBuilder, debugLogProvider, handler, commandReaderFactory ) );
        }
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
        NeedsInactivityTimeout pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder );
    }

    public interface NeedsScheduler
    {
        NeedsConfig scheduler( JobScheduler scheduler );
    }

    public interface NeedsConfig
    {
        NeedBootstrapConfig config( Config config );
    }

    public interface NeedsInactivityTimeout
    {
        NeedsScheduler inactivityTimeout( Duration inactivityTimeout );
    }

    public interface NeedBootstrapConfig
    {
        NeedCommandReader bootstrapConfig( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration );
    }

    public interface NeedCommandReader
    {
        AcceptsOptionalParams commandReader( CommandReaderFactory commandReaderFactory );
    }

    public interface AcceptsOptionalParams
    {
        AcceptsOptionalParams handShakeTimeout( Duration handshakeTimeout );
        AcceptsOptionalParams clock( Clock clock );
        AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider );
        CatchupClientFactory build();
    }
}
