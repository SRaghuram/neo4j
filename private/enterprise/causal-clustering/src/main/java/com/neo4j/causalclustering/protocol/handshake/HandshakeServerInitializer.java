/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.SimpleNettyChannel;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.RejectedExecutionException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.configuration.CausalClusteringInternalSettings.inbound_connection_initialization_logging_enabled;

public class HandshakeServerInitializer extends ChannelInitializer<SocketChannel>
{
    private final Log log;
    private final ApplicationProtocolRepository applicationProtocolRepository;
    private final ModifierProtocolRepository modifierProtocolRepository;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Config config;

    public HandshakeServerInitializer( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository, NettyPipelineBuilderFactory pipelineBuilderFactory,
            LogProvider logProvider, Config config )
    {
        this.log = logProvider.getLog( getClass() );
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.protocolInstallerRepository = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.config = config;
    }

    @Override
    public void initChannel( SocketChannel ch )
    {
        if ( config.get( inbound_connection_initialization_logging_enabled ) )
        {
            log.info( "Installing handshake server on channel %s", ch );
        }

        pipelineBuilderFactory.server( ch, log )
                .addFraming()
                .add( "handshake_server_encoder", new ServerMessageEncoder() )
                .add( "handshake_server_decoder", new ServerMessageDecoder() )
                .add( "handshake_server", createHandshakeServer( ch ) )
                .install();
    }

    private NettyHandshakeServer createHandshakeServer( SocketChannel channel )
    {
        HandshakeServer handshakeServer = new HandshakeServer(
                applicationProtocolRepository,
                modifierProtocolRepository,
                new SimpleNettyChannel( channel, log )
        );

        handshakeServer.protocolStackFuture().whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
        return new NettyHandshakeServer( handshakeServer );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, SocketChannel channel, Throwable failure )
    {
        if ( failure != null )
        {
            log.error( String.format( "Error when negotiating protocol stack on channel %s", channel ), failure );
            return;
        }

        try
        {
            log.info( "Handshake completed on channel %s. Installing: %s", channel, protocolStack );

            protocolInstallerRepository.installerFor( protocolStack ).install( channel );
            registerChannelAndProtocol( channel, protocolStack );
        }
        catch ( Throwable t )
        {
            log.error( String.format( "Error installing protocol stack on channel %s", channel ), t );
        }
    }

    private void registerChannelAndProtocol( SocketChannel channel, ProtocolStack protocolStack )
    {
        SocketAddress remoteAddress = getRemoteAddress( channel );
        if ( remoteAddress == null )
        {
            // already disconnected
            return;
        }

        ChannelPipeline parent = channel.parent().pipeline();
        parent.fireUserEventTriggered( new ServerHandshakeFinishedEvent.Created( remoteAddress, protocolStack ) );

        channel.closeFuture().addListener( f ->
        {
            try
            {
                parent.fireUserEventTriggered( new ServerHandshakeFinishedEvent.Closed( remoteAddress ) );
            }
            catch ( RejectedExecutionException ignored )
            {
            }
        } );
    }

    private SocketAddress getRemoteAddress( SocketChannel channel )
    {
        InetSocketAddress remoteAddress = channel.remoteAddress();
        if ( remoteAddress == null )
        {
            // this has been observed to happen when a connection is closed immediately after having been opened, i.e. port probing
            return null;
        }
        return new SocketAddress( remoteAddress.getHostString(), remoteAddress.getPort() );
    }
}
