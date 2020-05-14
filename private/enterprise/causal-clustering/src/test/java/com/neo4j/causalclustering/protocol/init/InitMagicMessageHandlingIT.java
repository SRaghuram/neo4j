/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;

import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.magicValueBuf;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.conditions.Conditions.FALSE;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

class InitMagicMessageHandlingIT
{
    private static final Duration SHORT_TIMEOUT = Duration.ofSeconds( 1 );
    private static final Duration LONG_TIMEOUT = Duration.ofHours( 1 );

    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @AfterEach
    void afterEach()
    {
        eventLoopGroup.shutdownGracefully( 1, 5, SECONDS );
    }

    @Test
    void serverShouldDropConnectionWhenClientIsInactive() throws Exception
    {
        var serverInitializer = newServerChannelInitializer( SHORT_TIMEOUT, new NoOpChannelInitializer() );
        var clientInitializer = new NoOpChannelInitializer();

        var clientChannel = startServerAndClient( serverInitializer, clientInitializer );

        assertEventuallyClosed( clientChannel );
        assertReadTimeoutLogged( ServerChannelInitializer.class );
    }

    @Test
    void serverShouldRespondWithInitMagicMessage() throws Exception
    {
        var serverInitializer = newServerChannelInitializer( LONG_TIMEOUT, new NoOpChannelInitializer() );

        var recordingClientHandler = new RecordingHandler();
        var clientInitializer = new ChannelInitializer<>()
        {
            @Override
            protected void initChannel( Channel ch )
            {
                ch.pipeline().addLast( recordingClientHandler );
            }
        };

        var clientChannel = startServerAndClient( serverInitializer, clientInitializer );
        assertTrue( clientChannel.isOpen() );

        clientChannel.writeAndFlush( magicValueBuf() );

        assertCorrectInitMessageReceived( "Client", recordingClientHandler );
        assertTrue( clientChannel.isActive() );
    }

    @Test
    void serverShouldDropConnectionWhenWrongInitMagicMessageReceived() throws Exception
    {
        var serverInitializer = newServerChannelInitializer( LONG_TIMEOUT, new NoOpChannelInitializer() );
        var clientInitializer = new NoOpChannelInitializer();

        var clientChannel = startServerAndClient( serverInitializer, clientInitializer );
        assertTrue( clientChannel.isOpen() );

        clientChannel.writeAndFlush( wrongMagicMessage() );

        assertEventuallyClosed( clientChannel );
        assertWrongMagicValueLogged( ServerChannelInitializer.class );
    }

    @Test
    void clientShouldDropConnectionWhenServerDoesNotRespond() throws Exception
    {
        var serverInitializer = new NoOpChannelInitializer();
        var clientInitializer = newClientChannelInitializer( SHORT_TIMEOUT, new NoOpChannelInitializer() );

        var clientChannel = startServerAndClient( serverInitializer, clientInitializer );

        assertEventuallyClosed( clientChannel );
        assertReadTimeoutLogged( ClientChannelInitializer.class );
    }

    @Test
    void clientShouldDropConnectionWhenWrongInitMagicMessageReceived() throws Exception
    {
        var serverInitializer = new ChannelInitializer<>()
        {
            @Override
            protected void initChannel( Channel ch )
            {
                ch.writeAndFlush( wrongMagicMessage() );
            }
        };
        var clientInitializer = newClientChannelInitializer( LONG_TIMEOUT, new NoOpChannelInitializer() );

        var clientChannel = startServerAndClient( serverInitializer, clientInitializer );

        assertEventuallyClosed( clientChannel );
        assertWrongMagicValueLogged( ClientChannelInitializer.class );
    }

    @Test
    void clientShouldSendMagicMessageWhenItBecomesActive() throws Exception
    {
        var recordingServerHandler = new RecordingHandler();
        var serverInitializer = new ChannelInitializer<>()
        {
            @Override
            protected void initChannel( Channel ch )
            {
                ch.pipeline().addLast( recordingServerHandler );
            }
        };
        var clientInitializer = newClientChannelInitializer( LONG_TIMEOUT, new NoOpChannelInitializer() );

        var clientChannel = startServerAndClient( serverInitializer, clientInitializer );
        assertTrue( clientChannel.isOpen() );

        assertCorrectInitMessageReceived( "Server", recordingServerHandler );
        assertTrue( clientChannel.isActive() );
    }

    private Channel startServerAndClient( ServerChannelInitializer serverInitializer, ChannelInitializer<?> clientInitializer ) throws Exception
    {
        return startServerAndClient( serverInitializer.asChannelInitializer(), clientInitializer );
    }

    private Channel startServerAndClient( ChannelInitializer<?> serverInitializer, ChannelInitializer<?> clientInitializer ) throws Exception
    {
        var server = new Server( serverInitializer );
        var client = new Client( clientInitializer );

        var serverAddress = server.start();
        var clientChannel = client.connect( serverAddress );

        return clientChannel;
    }

    private ServerChannelInitializer newServerChannelInitializer( Duration timeout, ChannelInitializer<?> initializer )
    {
        return new ServerChannelInitializer( initializer, NettyPipelineBuilderFactory.insecure(), timeout, logProvider, Config.defaults() );
    }

    private ClientChannelInitializer newClientChannelInitializer( Duration timeout, ChannelInitializer<?> initializer )
    {
        return new ClientChannelInitializer( initializer, NettyPipelineBuilderFactory.insecure(), timeout, logProvider );
    }

    private static void assertEventuallyClosed( Channel clientChannel )
    {
        assertEventually( "Server did not drop the connection", clientChannel::isActive, FALSE, 1, MINUTES );
    }

    private void assertReadTimeoutLogged( Class<?> logClass )
    {
        assertEventually( () -> "Read timeout error did not get logged:\n" + logProvider.serialize(),
                () -> safeTry( () -> assertThat( logProvider ).forClass( logClass ).forLevel( ERROR )
                        .assertExceptionForLogMessage( "Exception in inbound" )
                        .isInstanceOf( ReadTimeoutException.class ) ), TRUE, 1, MINUTES );
    }

    private void assertWrongMagicValueLogged( Class<?> logClass )
    {
        assertEventually( () -> "Wrong magic value did not get logged:\n" + logProvider.serialize(),
                () -> safeTry( () -> assertThat( logProvider ).forClass( logClass ).forLevel( ERROR )
                        .assertExceptionForLogMessage( "Exception in inbound" )
                        .isInstanceOf( DecoderException.class )
                        .hasMessageContaining( "Wrong magic value" ) ), TRUE, 1, MINUTES );
    }

    private boolean safeTry( Runnable runnable )
    {
        try
        {
            runnable.run();
            return true;
        }
        catch ( Exception e )
        {
            return false;
        }
    }

    private static void assertCorrectInitMessageReceived( String side, RecordingHandler recordingHandler )
    {
        assertEventually( side + " did not receive a magic message", () -> recordingHandler.messages, value -> value.contains( magicValueBuf() ), 1, MINUTES );
    }

    private static ByteBuf wrongMagicMessage()
    {
        var length = magicValueBuf().writerIndex();
        var bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes( bytes );
        return Unpooled.wrappedBuffer( bytes );
    }

    private class Server
    {
        final ChannelInitializer<?> initializer;

        Server( ChannelInitializer<?> initializer )
        {
            this.initializer = initializer;
        }

        SocketAddress start() throws InterruptedException
        {
            var bootstrap = new ServerBootstrap()
                    .group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .childHandler( initializer );

            return bootstrap.bind( 0 ).sync().channel().localAddress();
        }
    }

    private class Client
    {
        final ChannelInitializer<?> initializer;

        Client( ChannelInitializer<?> initializer )
        {
            this.initializer = initializer;
        }

        Channel connect( SocketAddress serverAddress ) throws InterruptedException
        {
            var bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( initializer );

            return bootstrap.connect( serverAddress ).sync().channel();
        }
    }

    private static class RecordingHandler extends ChannelInboundHandlerAdapter
    {
        final List<Object> messages = new CopyOnWriteArrayList<>();

        @Override
        public void channelRead( ChannelHandlerContext ctx, Object msg )
        {
            messages.add( msg );
        }
    }
}
