/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ssl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import java.net.InetSocketAddress;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SecureServer
{
    public static final byte[] RESPONSE = {5, 6, 7, 8};

    private SslContext sslContext;
    private Channel channel;
    private NioEventLoopGroup eventLoopGroup;

    public SecureServer( SslPolicy sslPolicy ) throws SSLException
    {
        this.sslContext = sslPolicy.nettyServerContext();
    }

    public void start()
    {
        eventLoopGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( eventLoopGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( 0 )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();
                        SSLEngine sslEngine = sslContext.newEngine( ch.alloc() );
                        SslHandler sslHandler = new SslHandler( sslEngine );
                        pipeline.addLast( sslHandler );

                        pipeline.addLast( new Responder() );
                    }
                } );

        channel = bootstrap.bind().syncUninterruptibly().channel();
    }

    public void stop()
    {
        channel.close().awaitUninterruptibly();
        channel = null;
        eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
    }

    public int port()
    {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    static class Responder extends SimpleChannelInboundHandler<ByteBuf>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
            ctx.channel().writeAndFlush( ctx.alloc().buffer().writeBytes( RESPONSE ) );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
        {
            //cause.printStackTrace(); // for debugging
        }
    }
}
