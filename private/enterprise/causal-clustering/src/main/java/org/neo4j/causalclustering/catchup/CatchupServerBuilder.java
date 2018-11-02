/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;

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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;

public class CatchupServerBuilder
{
    private final CatchupServerHandler catchupServerHandler;
    private LogProvider debugLogProvider = NullLogProvider.getInstance();
    private LogProvider userLogProvider = NullLogProvider.getInstance();
    private NettyPipelineBuilderFactory pipelineBuilder = new NettyPipelineBuilderFactory( VOID_WRAPPER );
    private ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
    private Collection<ModifierSupportedProtocols> modifierProtocols = emptyList();
    private ChannelInboundHandler parentHandler;
    private ListenSocketAddress listenAddress;
    private String serverName = "catchup-server";

    public CatchupServerBuilder( CatchupServerHandler catchupServerHandler )
    {
        this.catchupServerHandler = catchupServerHandler;
    }

    public CatchupServerBuilder catchupProtocols( ApplicationSupportedProtocols catchupProtocols )
    {
        this.catchupProtocols = catchupProtocols;
        return this;
    }

    public CatchupServerBuilder modifierProtocols( Collection<ModifierSupportedProtocols> modifierProtocols )
    {
        this.modifierProtocols = modifierProtocols;
        return this;
    }

    public CatchupServerBuilder pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder )
    {
        this.pipelineBuilder = pipelineBuilder;
        return this;
    }

    public CatchupServerBuilder serverHandler( ChannelInboundHandler parentHandler )
    {
        this.parentHandler = parentHandler;
        return this;
    }

    public CatchupServerBuilder listenAddress( ListenSocketAddress listenAddress )
    {
        this.listenAddress = listenAddress;
        return this;
    }

    public CatchupServerBuilder userLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
        return this;
    }

    public CatchupServerBuilder debugLogProvider( LogProvider debugLogProvider )
    {
        this.debugLogProvider = debugLogProvider;
        return this;
    }

    public CatchupServerBuilder serverName( String serverName )
    {
        this.serverName = serverName;
        return this;
    }

    public Server build()
    {
        ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

        CatchupProtocolServerInstaller.Factory catchupProtocolServerInstaller = new CatchupProtocolServerInstaller.Factory( pipelineBuilder, debugLogProvider,
                catchupServerHandler );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                singletonList( catchupProtocolServerInstaller ), ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilder, debugLogProvider );

        return new Server( handshakeServerInitializer, parentHandler, debugLogProvider, userLogProvider, listenAddress, serverName );
    }
}
