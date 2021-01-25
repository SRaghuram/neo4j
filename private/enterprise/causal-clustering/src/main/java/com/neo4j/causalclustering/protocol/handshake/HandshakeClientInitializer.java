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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.protocol.handshake.ChannelAttribute.PROTOCOL_STACK;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;

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
        this.handshakeDelay = exponential( 1, 2000, MILLISECONDS );
    }

    @Override
    protected void initChannel( SocketChannel channel )
    {
        HandshakeClient handshakeClient = newHandshakeClient( channel );

        try
        {
            installHandlers( channel, handshakeClient );
        }
        catch ( Exception e )
        {
            handshakeClient.protocol().completeExceptionally( e );
        }

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
        debugLog.info( "Initiating handshake on channel %s", channel );

        SimpleNettyChannel channelWrapper = new SimpleNettyChannel( channel, debugLog );
        handshakeClient.initiate( channelWrapper, applicationProtocolRepository, modifierProtocolRepository );
        handshakeClient.protocol().whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, Channel channel, Throwable failure )
    {
        if ( failure != null )
        {
            debugLog.error( String.format( "Error when negotiating protocol stack on channel %s", channel ), failure );
            channel.pipeline().fireUserEventTriggered( GateEvent.getFailure() );
            channel.close();
        }
        else
        {
            try
            {
                userLog( protocolStack, channel );

                debugLog.info( "Handshake completed on channel %s. Installing: %s", channel, protocolStack );
                protocolInstaller.installerFor( protocolStack ).install( channel );

                channel.pipeline().fireUserEventTriggered( GateEvent.getSuccess() );
                channel.flush();
            }
            catch ( Exception e )
            {
                debugLog.error( String.format( "Error installing protocol stack on channel %s", channel ), e );
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

    private static HandshakeClient newHandshakeClient( Channel channel )
    {
        CompletableFuture<ProtocolStack> protocolFuture = channel.attr( PROTOCOL_STACK ).get();
        if ( protocolFuture == null )
        {
            throw new IllegalStateException( "Channel " + channel + " does not contain a protocol stack attribute" );
        }
        return new HandshakeClient( protocolFuture );
    }
}
