/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.v1.CatchupProtocolClientInstallerV1;
import org.neo4j.causalclustering.catchup.v2.CatchupProtocolClientInstallerV2;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Client;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
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

    private static class StepBuilder implements NeedsDefaultDatabaseName, NeedsCatchupProtocols,
            NeedsModifierProtocols, NeedsPipelineBuilder, NeedsInactivityTimeout, NeedsScheduler, AcceptsOptionalParams
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
        public AcceptsOptionalParams scheduler( JobScheduler scheduler )
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
        public CatchupClientFactory build()
        {
            ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
            ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

            Function<CatchupResponseHandler,HandshakeClientInitializer> channelInitializerFactory = handler -> {
                List<ProtocolInstaller.Factory<Client,?>> installers = Arrays.asList(
                        new CatchupProtocolClientInstallerV1.Factory( pipelineBuilder, debugLogProvider, handler ),
                        new CatchupProtocolClientInstallerV2.Factory( pipelineBuilder, debugLogProvider, handler )
                );

                ProtocolInstallerRepository<Client> protocolInstallerRepository = new ProtocolInstallerRepository<>( installers,
                        ModifierProtocolInstaller.allClientInstallers );

                return new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository, protocolInstallerRepository, pipelineBuilder,
                        handshakeTimeout, debugLogProvider, userLogProvider );
            };

            return new CatchupClientFactory( debugLogProvider, clock, channelInitializerFactory, defaultDatabaseName, inactivityTimeout, scheduler );
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
        AcceptsOptionalParams scheduler( JobScheduler scheduler );
    }

    public interface NeedsInactivityTimeout
    {
        NeedsScheduler inactivityTimeout( Duration inactivityTimeout );
    }

    public interface AcceptsOptionalParams
    {
        AcceptsOptionalParams handShakeTimeout( Duration handshakeTimeout );
        AcceptsOptionalParams clock( Clock clock );
        AcceptsOptionalParams debugLogProvider( LogProvider debugLogProvider );
        AcceptsOptionalParams userLogProvider( LogProvider userLogProvider );
        CatchupClientFactory build();
    }
}
