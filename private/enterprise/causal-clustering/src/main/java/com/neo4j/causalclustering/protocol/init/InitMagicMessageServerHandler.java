/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class InitMagicMessageServerHandler extends SimpleChannelInboundHandler<InitialMagicMessage>
{
    static final String NAME = "init_magic_message_server_handler";

    private final ChannelInitializer<?> handshakeInitializer;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final LogProvider logProvider;
    private final Log log;

    InitMagicMessageServerHandler( ChannelInitializer<?> handshakeInitializer, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
    {
        this.handshakeInitializer = handshakeInitializer;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, InitialMagicMessage msg ) throws Exception
    {
        var channel = ctx.channel();

        if ( msg.isCorrectMagic() )
        {
            log.debug( "Channel %s received a correct initialization message", channel );

            // write the same magic message back to the client so that it knows we are a valid neo4j server
            // use void promise that fires errors back into the pipeline where they are handled by the installed error handlers
            channel.writeAndFlush( msg, channel.voidPromise() );

            // install different handlers into the pipeline to handle protocol handshake
            pipelineBuilderFactory.server( channel, logProvider.getLog( handshakeInitializer.getClass() ) )
                    .addFraming()
                    .add( "handshake_initializer", handshakeInitializer )
                    .install();
        }
        else
        {
            ctx.close();
            throw new IllegalStateException( "Illegal initialization message: " + msg );
        }
    }
}
