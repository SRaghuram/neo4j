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

class InitServerHandler extends ReplayingDecoder<Void>
{
    static final String NAME = "init_server_handler";

    private final ChannelInitializer<?> handshakeInitializer;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final LogProvider logProvider;
    private final Log log;

    InitServerHandler( ChannelInitializer<?> handshakeInitializer, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
    {
        this.handshakeInitializer = handshakeInitializer;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        var receivedMagicValue = readMagicValue( in );

        if ( isCorrectMagicValue( receivedMagicValue ) )
        {
            var channel = ctx.channel();

            log.debug( "Channel %s received a correct magic message", channel );

            // write the same magic message back to the client so that it knows we are a valid neo4j server
            // use void promise that fires errors back into the pipeline where they are handled by the installed error handlers
            channel.writeAndFlush( magicValueBuf(), channel.voidPromise() );

            // install different handlers into the pipeline to handle protocol handshake
            pipelineBuilderFactory.server( channel, logProvider.getLog( handshakeInitializer.getClass() ) )
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
