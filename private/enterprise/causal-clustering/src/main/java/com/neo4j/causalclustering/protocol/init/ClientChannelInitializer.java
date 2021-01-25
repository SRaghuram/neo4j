/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ChannelAttribute;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link ChannelInitializer} for client connections. It performs an initial handshake using a {@link MagicValueUtil#magicValueBuf()}
 * and then installs a different {@link ChannelInitializer}. The next initializer is supposed to handle further protocol negotiations.
 *
 * @see HandshakeClientInitializer
 */
public class ClientChannelInitializer extends ChannelInitializer<Channel>
{
    private final ChannelInitializer<?> handshakeInitializer;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Duration timeout;
    private final LogProvider logProvider;
    private final Log log;

    public ClientChannelInitializer( ChannelInitializer<?> handshakeInitializer, NettyPipelineBuilderFactory pipelineBuilderFactory,
            Duration timeout, LogProvider logProvider )
    {
        this.handshakeInitializer = handshakeInitializer;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.timeout = timeout;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void initChannel( Channel channel )
    {
        log.info( "Initializing client channel %s", channel );

        var protocolFuture = new CompletableFuture<ProtocolStack>();
        channel.attr( ChannelAttribute.PROTOCOL_STACK ).set( protocolFuture );
        channel.closeFuture().addListener( ignore -> protocolFuture.completeExceptionally( new ClosedChannelException() ) );

        pipelineBuilderFactory.client( channel, log )
                .add( "read_timeout_handler", new ReadTimeoutHandler( timeout.toMillis(), MILLISECONDS ) )
                .add( InitClientHandler.NAME, new InitClientHandler( handshakeInitializer, pipelineBuilderFactory, logProvider ) )
                .install();
    }
}
