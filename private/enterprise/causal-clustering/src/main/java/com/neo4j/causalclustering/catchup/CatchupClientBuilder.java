/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v1.CatchupProtocolClientInstallerV1;
import com.neo4j.causalclustering.catchup.v2.CatchupProtocolClientInstallerV2;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.catchup.v3.storecopy.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import io.netty.channel.socket.SocketChannel;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.time.Clocks.systemClock;

public final class CatchupClientBuilder
{
    private CatchupClientBuilder()
    {
    }

    public static NeedsDefaultDatabaseName builder()
    {
        return new StepBuilder();
    }

    private static class StepBuilder implements NeedsDefaultDatabaseName, NeedsCatchupProtocols, NeedsModifierProtocols, NeedsPipelineBuilder,
            NeedsInactivityTimeout, NeedsScheduler, NeedBootstrapConfig, AcceptsOptionalParams
    {
        private String defaultDatabaseName;
        private NettyPipelineBuilderFactory pipelineBuilder;
        private ApplicationSupportedProtocols catchupProtocols;
        private Collection<ModifierSupportedProtocols> modifierProtocols;
        private JobScheduler scheduler;
        private LogProvider debugLogProvider = NullLogProvider.getInstance();
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private Duration inactivityTimeout;
        private Duration handshakeTimeout = Duration.ofSeconds( 5 );
        private Clock clock = systemClock();
        private BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration;

        private StepBuilder()
        {
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
        public NeedBootstrapConfig scheduler( JobScheduler scheduler )
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
        public AcceptsOptionalParams userLogProvider( LogProvider userLogProvider )
        {
            this.userLogProvider = userLogProvider;
            return this;
        }

        @Override
        public AcceptsOptionalParams bootstrapConfig( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration )
        {
            this.bootstrapConfiguration = bootstrapConfiguration;
            return this;
        }

        @Override
        public CatchupClientFactory build( Consumer<Lifecycle> lifecycleHandler )
        {
            ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
            ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

            Function<CatchupResponseHandler,HandshakeClientInitializer> channelInitializerFactory = handler -> {
                List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Client,?>> installers = Arrays.asList(
                        new CatchupProtocolClientInstallerV1.Factory( pipelineBuilder, debugLogProvider, handler ),
                        new CatchupProtocolClientInstallerV2.Factory( pipelineBuilder, debugLogProvider, handler ),
                        new CatchupProtocolClientInstallerV3.Factory( pipelineBuilder, debugLogProvider, handler )
                );

                ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>( installers,
                        ModifierProtocolInstaller.allClientInstallers );

                return new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository, protocolInstallerRepository, pipelineBuilder,
                        handshakeTimeout, debugLogProvider, userLogProvider );
            };

            CatchupChannelPoolService
                    catchupChannelPoolService = new CatchupChannelPoolService( bootstrapConfiguration, scheduler, clock, channelInitializerFactory );
            lifecycleHandler.accept( catchupChannelPoolService );

            return new CatchupClientFactory( defaultDatabaseName, inactivityTimeout, catchupChannelPoolService );
        }
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
        NeedsInactivityTimeout pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder );
    }

    public interface NeedsScheduler
    {
        NeedBootstrapConfig scheduler( JobScheduler scheduler );
    }

    public interface NeedsInactivityTimeout
    {
        NeedsScheduler inactivityTimeout( Duration inactivityTimeout );
    }

    public interface NeedBootstrapConfig
    {
        AcceptsOptionalParams bootstrapConfig( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration );
    }

    public interface AcceptsOptionalParams
    {
        AcceptsOptionalParams handShakeTimeout( Duration handshakeTimeout );
        AcceptsOptionalParams clock( Clock clock );
        AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider );
        AcceptsOptionalParams userLogProvider( LogProvider userLogProvider );

        CatchupClientFactory build( Consumer<Lifecycle> lifecycleHandler );
    }
}
