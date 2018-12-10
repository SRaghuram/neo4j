/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.SimpleNettyChannel;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.NullLog;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class NettyProtocolHandshakeIT
{
    private ApplicationSupportedProtocols supportedRaftApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, emptyList() );
    private ApplicationSupportedProtocols supportedCatchupApplicationProtocol =
            new ApplicationSupportedProtocols( CATCHUP, emptyList() );
    private Collection<ModifierSupportedProtocols> supportedCompressionModifierProtocols =
            singletonList( new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ) );
    private Collection<ModifierSupportedProtocols> noSupportedModifierProtocols = emptyList();

    private ApplicationProtocolRepository raftApplicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedRaftApplicationProtocol );
    private ApplicationProtocolRepository catchupApplicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedCatchupApplicationProtocol );
    private ModifierProtocolRepository compressionModifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), supportedCompressionModifierProtocols );
    private ModifierProtocolRepository unsupportingModifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), noSupportedModifierProtocols );

    private Server server;
    private HandshakeClient handshakeClient;
    private Client client;

    @Before
    public void setUp()
    {
        server = new Server();
        server.start( raftApplicationProtocolRepository, compressionModifierProtocolRepository );

        handshakeClient = new HandshakeClient();

        client = new Client( handshakeClient );
        client.connect( server.port() );
    }

    @After
    public void tearDown()
    {
        client.disconnect();
        server.stop();
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClientWithCompression() throws Exception
    {
        // when
        handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository,
                compressionModifierProtocolRepository );

        // then
        ProtocolStack clientProtocolStack = handshakeClient.protocol().get( 1, TimeUnit.MINUTES );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT) ) );
        assertThat( clientProtocolStack.modifierProtocols(), contains( TestModifierProtocols.latest( COMPRESSION ) ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServerWithCompression() throws Exception
    {
        // when
        handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository,
                compressionModifierProtocolRepository );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( handshakeClient.protocol() );

        // then
        ProtocolStack serverProtocolStack = serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT ) ) );
        assertThat( serverProtocolStack.modifierProtocols(), contains( TestModifierProtocols.latest( COMPRESSION ) ) );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnClientNoModifiers() throws Exception
    {
        // when
        handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository,
                unsupportingModifierProtocolRepository );
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.protocol();

        // then
        ProtocolStack clientProtocolStack = clientHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT) ) );
        assertThat( clientProtocolStack.modifierProtocols(), empty() );
    }

    @Test
    public void shouldSuccessfullyHandshakeKnownProtocolOnServerNoModifiers() throws Exception
    {
        // when
        handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), raftApplicationProtocolRepository,
                unsupportingModifierProtocolRepository );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( handshakeClient.protocol() );

        // then
        ProtocolStack serverProtocolStack = serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( TestApplicationProtocols.latest( RAFT ) ) );
        assertThat( serverProtocolStack.modifierProtocols(), empty() );
    }

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnClient() throws Throwable
    {
        // when
        handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), catchupApplicationProtocolRepository,
                compressionModifierProtocolRepository );

        // then
        try
        {
            handshakeClient.protocol().get( 1, TimeUnit.MINUTES );
        }
        catch ( ExecutionException ex )
        {
            throw ex.getCause();
        }
    }

    @Test( expected = ServerHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnServer() throws Throwable
    {
        // when
        handshakeClient.initiate( new SimpleNettyChannel( client.channel, NullLog.getInstance() ), catchupApplicationProtocolRepository,
                compressionModifierProtocolRepository );

        CompletableFuture<ProtocolStack> serverHandshakeFuture = getServerHandshakeFuture( handshakeClient.protocol() );

        // then
        try
        {
            serverHandshakeFuture.get( 1, TimeUnit.MINUTES );
        }
        catch ( ExecutionException ex )
        {
            throw ex.getCause();
        }
    }

    /**
     * Only attempt to access handshakeServer when client has completed, and do so whether client has completed normally or exceptionally
     * This is to avoid NullPointerException if handshakeServer accessed too soon
     */
    private CompletableFuture<ProtocolStack> getServerHandshakeFuture( CompletableFuture<ProtocolStack> clientFuture )
    {
        return clientFuture.handle( ( ignoreSuccess, ignoreFailure ) -> null ).thenCompose( ignored -> server.handshakeServer.protocolStackFuture() );
    }

    private static class Server
    {
        Channel channel;
        NioEventLoopGroup eventLoopGroup;
        HandshakeServer handshakeServer;

        void start( final ApplicationProtocolRepository applicationProtocolRepository, final ModifierProtocolRepository modifierProtocolRepository )
        {
            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap().group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( 0 )
                    .childHandler( new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel( SocketChannel ch )
                        {
                            ChannelPipeline pipeline = ch.pipeline();
                            handshakeServer = new HandshakeServer(
                                    applicationProtocolRepository, modifierProtocolRepository, new SimpleNettyChannel( ch, NullLog.getInstance() ) );
                            pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
                            pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                            pipeline.addLast( "responseMessageEncoder", new ServerMessageEncoder() );
                            pipeline.addLast( "requestMessageDecoder", new ServerMessageDecoder() );
                            pipeline.addLast( new NettyHandshakeServer( handshakeServer ) );
                        }
                    } );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        void stop()
        {
            channel.close().awaitUninterruptibly();
            channel = null;
            eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
        }

        int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }
    }

    private static class Client
    {
        Bootstrap bootstrap;
        NioEventLoopGroup eventLoopGroup;
        Channel channel;

        Client( HandshakeClient handshakeClient )
        {
            eventLoopGroup = new NioEventLoopGroup();
            bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).handler( new ClientInitializer( handshakeClient ) );
        }

        @SuppressWarnings( "SameParameterValue" )
        void connect( int port )
        {
            ChannelFuture channelFuture = bootstrap.connect( "localhost", port ).awaitUninterruptibly();
            channel = channelFuture.channel();
        }

        void disconnect()
        {
            if ( channel != null )
            {
                channel.close().awaitUninterruptibly();
                eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
            }
        }
    }

    static class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        private final HandshakeClient handshakeClient;

        ClientInitializer( HandshakeClient handshakeClient )
        {
            this.handshakeClient = handshakeClient;
        }

        @Override
        protected void initChannel( SocketChannel channel )
        {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
            pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
            pipeline.addLast( "requestMessageEncoder", new ClientMessageEncoder() );
            pipeline.addLast( "responseMessageDecoder", new ClientMessageDecoder() );
            pipeline.addLast( new NettyHandshakeClient( handshakeClient ) );
        }
    }
}
