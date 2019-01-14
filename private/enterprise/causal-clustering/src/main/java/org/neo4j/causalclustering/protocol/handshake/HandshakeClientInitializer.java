/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.protocol.handshake.ChannelAttribute.PROTOCOL_STACK;

public class HandshakeClientInitializer extends ChannelInitializer<SocketChannel>
{
    private final ApplicationProtocolRepository applicationProtocolRepository;
    private final ModifierProtocolRepository modifierProtocolRepository;
    private final Duration timeout;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final TimeoutStrategy handshakeDelay;
    private final Log debugLog;
    private final Log userLog;

    public HandshakeClientInitializer( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository, NettyPipelineBuilderFactory pipelineBuilderFactory,
            Duration handshakeTimeout, LogProvider debugLogProvider, LogProvider userLogProvider )
    {
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.timeout = handshakeTimeout;
        this.protocolInstaller = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.handshakeDelay = new ExponentialBackoffStrategy( 1, 2000, MILLISECONDS );
    }

    @Override
    protected void initChannel( SocketChannel channel )
    {
        HandshakeClient handshakeClient = new HandshakeClient();

        /* We store the Future<ProtocolStack> in a Netty channel attr to give us access to it at a higher level. For example,
        we need to know which protocol has been agreed upon when deciding which version of a catchup request to execute (see CatchupClient). */
        channel.attr( PROTOCOL_STACK ).set( handshakeClient.protocol() );
        channel.closeFuture().addListener( ignored -> handshakeClient.protocol().completeExceptionally( new ClosedChannelException().fillInStackTrace() ) );

        try
        {
            installHandlers( channel, handshakeClient );
        }
        catch ( Exception e )
        {
            handshakeClient.protocol().completeExceptionally( e );
        }

        debugLog.info( "Scheduling handshake (and timeout) local %s remote %s", channel.localAddress(), channel.remoteAddress() );

        scheduleHandshake( channel, handshakeClient, handshakeDelay.newTimeout() );
        scheduleTimeout( channel, handshakeClient );
    }

    private void installHandlers( Channel channel, HandshakeClient handshakeClient ) throws Exception
    {
        pipelineBuilderFactory.client( channel, debugLog )
                .addFraming()
                .add( "handshake_client_encoder", new ClientMessageEncoder() )
                .add( "handshake_client_decoder", new ClientMessageDecoder() )
                .add( "handshake_client", new NettyHandshakeClient( handshakeClient ) )
                .addGate( msg -> !(msg instanceof ServerMessage) )
                .install();
    }

    /**
     * Schedules the handshake initiation after the connection attempt.
     */
    private void scheduleHandshake( SocketChannel ch, HandshakeClient handshakeClient, TimeoutStrategy.Timeout handshakeDelay )
    {
        ch.eventLoop().schedule( () ->
        {
            if ( ch.isActive() )
            {
                initiateHandshake( ch, handshakeClient );
            }
            else if ( ch.isOpen() )
            {
                handshakeDelay.increment();
                scheduleHandshake( ch, handshakeClient, handshakeDelay );
            }
        }, handshakeDelay.getMillis(), MILLISECONDS );
    }

    private void scheduleTimeout( SocketChannel ch, HandshakeClient handshakeClient )
    {
        ch.eventLoop().schedule( () ->
        {
            handshakeClient.protocol().completeExceptionally( new TimeoutException( "Handshake timed out after " + timeout ) );
        }, timeout.toMillis(), TimeUnit.MILLISECONDS );
    }

    private void initiateHandshake( Channel channel, HandshakeClient handshakeClient )
    {
        debugLog.info( "Initiating handshake local %s remote %s", channel.localAddress(), channel.remoteAddress() );

        SimpleNettyChannel channelWrapper = new SimpleNettyChannel( channel, debugLog );
        handshakeClient.initiate( channelWrapper, applicationProtocolRepository, modifierProtocolRepository );
        handshakeClient.protocol().whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, Channel channel, Throwable failure )
    {
        if ( failure != null )
        {
            debugLog.error( "Error when negotiating protocol stack", failure );
            channel.pipeline().fireUserEventTriggered( GateEvent.getFailure() );
            channel.close();
        }
        else
        {
            try
            {
                userLog( protocolStack, channel );

                debugLog.info( "Installing " + protocolStack );
                protocolInstaller.installerFor( protocolStack ).install( channel );

                channel.pipeline().fireUserEventTriggered( GateEvent.getSuccess() );
                channel.flush();
            }
            catch ( Exception e )
            {
                debugLog.error( "Error installing pipeline", e );
                channel.close();
            }
        }
    }

    private void userLog( ProtocolStack protocolStack, Channel channel )
    {
        userLog.info( format(
                "Connected to %s [%s]", channel.remoteAddress(), protocolStack ) );
        channel.closeFuture().addListener( f -> userLog.info( format(
                "Lost connection to %s [%s]", channel.remoteAddress(), protocolStack ) ) );
    }
}
