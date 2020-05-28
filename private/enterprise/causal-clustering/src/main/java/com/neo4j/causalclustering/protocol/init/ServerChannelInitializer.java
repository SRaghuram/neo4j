/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.net.ChildInitializer;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.time.Duration;

import org.neo4j.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.configuration.CausalClusteringInternalSettings.inbound_connection_initialization_logging_enabled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link ChannelInitializer} for server connections. It performs an initial handshake using a {@link MagicValueUtil#magicValueBuf()}
 * and then installs a different {@link ChannelInitializer}. The next initializer is supposed to handle further protocol negotiations.
 *
 * @see HandshakeServerInitializer
 */
public class ServerChannelInitializer implements ChildInitializer
{
    private final ChannelInitializer<?> handshakeInitializer;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Duration timeout;
    private final LogProvider logProvider;
    private final Log log;
    private final Config config;

    public ServerChannelInitializer( ChannelInitializer<?> handshakeInitializer, NettyPipelineBuilderFactory pipelineBuilderFactory,
            Duration timeout, LogProvider logProvider, Config config )
    {
        this.handshakeInitializer = handshakeInitializer;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.timeout = timeout;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.config = config;
    }

    @Override
    public void initChannel( Channel channel )
    {
        if ( config.get( inbound_connection_initialization_logging_enabled ) )
        {
            log.info( "Initializing server channel %s", channel );
        }

        pipelineBuilderFactory.server( channel, log )
                .add( "read_timeout_handler", new ReadTimeoutHandler( timeout.toMillis(), MILLISECONDS ) )
                .add( InitServerHandler.NAME, new InitServerHandler( handshakeInitializer, pipelineBuilderFactory, logProvider ) )
                .install();
    }
}
