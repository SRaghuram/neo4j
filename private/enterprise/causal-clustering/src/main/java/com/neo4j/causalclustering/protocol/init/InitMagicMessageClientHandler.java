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

class InitMagicMessageClientHandler extends SimpleChannelInboundHandler<InitialMagicMessage>
{
    static final String NAME = "init_magic_message_client_handler";

    private final ChannelInitializer<?> handshakeInitializer;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final LogProvider logProvider;
    private final Log log;

    InitMagicMessageClientHandler( ChannelInitializer<?> handshakeInitializer, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
    {
        this.handshakeInitializer = handshakeInitializer;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelActive( ChannelHandlerContext ctx )
    {
        // write a magic message to the server once the channel is active
        // use void promise that fires errors back into the pipeline where they are handled by the installed error handlers
        ctx.writeAndFlush( InitialMagicMessage.instance(), ctx.voidPromise() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, InitialMagicMessage msg ) throws Exception
    {
        var channel = ctx.channel();

        if ( msg.isCorrectMagic() )
        {
            log.debug( "Channel %s received a correct initialization message", channel );

            // install different handlers into the pipeline to handle protocol handshake
            pipelineBuilderFactory.client( channel, logProvider.getLog( handshakeInitializer.getClass() ) )
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
