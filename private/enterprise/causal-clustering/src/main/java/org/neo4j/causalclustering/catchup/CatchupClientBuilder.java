/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static org.neo4j.time.Clocks.systemClock;

public class CatchupClientBuilder
{
    private Duration handshakeTimeout = Duration.ofSeconds( 5 );
    private LogProvider debugLogProvider = NullLogProvider.getInstance();
    private LogProvider userLogProvider = NullLogProvider.getInstance();
    private NettyPipelineBuilderFactory pipelineBuilder = new NettyPipelineBuilderFactory( VOID_WRAPPER );
    private ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
    private Collection<ModifierSupportedProtocols> modifierProtocols = emptyList();
    private Clock clock = systemClock();
    private long inactivityTimeoutMillis = TimeUnit.SECONDS.toMillis( 10 );

    public CatchupClientBuilder()
    {
    }

    public CatchupClientBuilder( ApplicationSupportedProtocols catchupProtocols, Collection<ModifierSupportedProtocols> modifierProtocols,
            NettyPipelineBuilderFactory pipelineBuilder, Duration handshakeTimeout, LogProvider debugLogProvider, LogProvider userLogProvider, Clock clock )
    {
        this.catchupProtocols = catchupProtocols;
        this.modifierProtocols = modifierProtocols;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeTimeout = handshakeTimeout;
        this.debugLogProvider = debugLogProvider;
        this.userLogProvider = userLogProvider;
        this.clock = clock;
    }

    public CatchupClientBuilder catchupProtocols( ApplicationSupportedProtocols catchupProtocols )
    {
        this.catchupProtocols = catchupProtocols;
        return this;
    }

    public CatchupClientBuilder modifierProtocols( Collection<ModifierSupportedProtocols> modifierProtocols )
    {
        this.modifierProtocols = modifierProtocols;
        return this;
    }

    public CatchupClientBuilder pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder )
    {
        this.pipelineBuilder = pipelineBuilder;
        return this;
    }

    public CatchupClientBuilder handshakeTimeout( Duration handshakeTimeout )
    {
        this.handshakeTimeout = handshakeTimeout;
        return this;
    }

    public CatchupClientBuilder inactivityTimeoutMillis( long inactivityTimeoutMillis )
    {
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        return this;
    }

    public CatchupClientBuilder debugLogProvider( LogProvider debugLogProvider )
    {
        this.debugLogProvider = debugLogProvider;
        return this;
    }

    public CatchupClientBuilder userLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
        return this;
    }

    public CatchupClientBuilder clock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    public CatchUpClient build()
    {
        ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

        Function<CatchUpResponseHandler,ChannelInitializer<SocketChannel>> channelInitializer = handler -> {
            List<ProtocolInstaller.Factory<Client,?>> installers = singletonList(
                    new CatchupProtocolClientInstaller.Factory( pipelineBuilder, debugLogProvider, handler ) );

            ProtocolInstallerRepository<Client> protocolInstallerRepository = new ProtocolInstallerRepository<>( installers,
                    ModifierProtocolInstaller.allClientInstallers );

            return new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository, protocolInstallerRepository, pipelineBuilder,
                    handshakeTimeout, debugLogProvider, userLogProvider );
        };

        return new CatchUpClient( debugLogProvider, clock, inactivityTimeoutMillis, channelInitializer );
    }
}
