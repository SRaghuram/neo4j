/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.isCorrectMagicValue;
import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.magicValueBuf;
import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.readMagicValue;

class InitClientHandler extends ReplayingDecoder<Void>
{
    static final String NAME = "init_client_handler";

    private final ChannelInitializer<?> handshakeInitializer;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final LogProvider logProvider;
    private final Log log;

    InitClientHandler( ChannelInitializer<?> handshakeInitializer, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
    {
        this.handshakeInitializer = handshakeInitializer;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        setSingleDecode( true );
    }

    @Override
    public void channelActive( ChannelHandlerContext ctx )
    {
        // write a magic message to the server once the channel is active
        // use void promise that fires errors back into the pipeline where they are handled by the installed error handlers
        ctx.writeAndFlush( magicValueBuf(), ctx.voidPromise() );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        var receivedMagicValue = readMagicValue( in );

        if ( isCorrectMagicValue( receivedMagicValue ) )
        {
            log.debug( "Channel %s received a correct magic message", ctx.channel() );

            // install different handlers into the pipeline to handle protocol handshake
            pipelineBuilderFactory.client( ctx.channel(), logProvider.getLog( handshakeInitializer.getClass() ) )
                    .addFraming()
                    .add( "handshake_initializer", handshakeInitializer )
                    .install();
        }
        else
        {
            ctx.close();
            throw new DecoderException( "Wrong magic value: '" + receivedMagicValue + "'" );
        }
    }
}
