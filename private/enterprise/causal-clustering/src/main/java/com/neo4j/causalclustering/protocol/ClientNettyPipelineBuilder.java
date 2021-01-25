/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import javax.net.ssl.SSLException;

import org.neo4j.logging.Log;
import org.neo4j.ssl.SslPolicy;

public class ClientNettyPipelineBuilder extends NettyPipelineBuilder<ProtocolInstaller.Orientation.Client, ClientNettyPipelineBuilder>
{
    private static final int LENGTH_FIELD_BYTES = 4;

    ClientNettyPipelineBuilder( ChannelPipeline pipeline, SslPolicy sslPolicy, Log log )
    {
        super( pipeline, sslPolicy, log );
    }

    @Override
    public ClientNettyPipelineBuilder addFraming()
    {
        add( "frame_encoder", new LengthFieldPrepender( LENGTH_FIELD_BYTES ) );
        add( "frame_decoder", new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, LENGTH_FIELD_BYTES, 0, LENGTH_FIELD_BYTES ) );
        return this;
    }

    @Override
    ChannelHandler createSslHandler( Channel channel, SslPolicy sslPolicy ) throws SSLException
    {
        return sslPolicy.nettyClientHandler( channel );
    }
}
