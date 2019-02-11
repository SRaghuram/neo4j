/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.protocol.handshake.ChannelAttribute;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.ServerSocketChannel;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static co.unruly.matchers.StreamMatchers.empty;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChannelPoolServiceIT
{
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    private static final int DEFAULT_TIME_OUT = 30;
    private final ProtocolStack protocolStackRaft = new ProtocolStack( TestApplicationProtocols.RAFT_2, emptyList() );
    private ChannelPoolService pool;
    private AdvertisedSocketAddress to1;
    private AdvertisedSocketAddress to2;
    private final AdvertisedSocketAddress serverlessAddress = new AdvertisedSocketAddress( "localhost", PortAuthority.allocatePort() );
    private EventLoopGroup serverEventExecutor;
    private PoolEventsMonitor poolEventsMonitor;

    @BeforeEach
    void setUpServers() throws ExecutionException, InterruptedException
    {
        poolEventsMonitor = new PoolEventsMonitor();
        pool = new ChannelPoolService( BootstrapConfiguration.clientConfig( Config.defaults() ), new ThreadPoolJobScheduler(), poolEventsMonitor );

        startServers();

        pool.start();
    }

    @AfterEach
    void tearDown()
    {
        closeServers();
        pool.stop();
    }

    @Test
    void shouldNotReleaseMoreThanOnce() throws InterruptedException, ExecutionException, TimeoutException
    {
        PooledChannel pooledChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        pooledChannel.release().get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
        assertThrows( IllegalStateException.class, () -> pooledChannel.release().get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );
    }

    @Test
    void shouldNotAllowGettingChannelIfItHasBeenScheduledForReleased() throws InterruptedException, ExecutionException, TimeoutException
    {
        PooledChannel pooledChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        pooledChannel.release();
        assertThrows( IllegalStateException.class, pooledChannel::channel );
    }

    @Test
    void shouldNotCreateAdditionalChannelIfCurrentHasBeenReleased() throws InterruptedException, ExecutionException, TimeoutException
    {
        PooledChannel pooledChannel;

        for ( int i = 0; i < 2; i++ )
        {
            pooledChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

            pooledChannel.release().get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
        }

        assertEquals( 1, poolEventsMonitor.created );
        assertEquals( 1, poolEventsMonitor.acquired );
        assertEquals( 2, poolEventsMonitor.released );
    }

    @Test
    void shouldCreateAdditionalChannelIfCurrentIsNotReleased() throws InterruptedException, ExecutionException, TimeoutException
    {
        PooledChannel notReleased = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        PooledChannel pooledChannel;

        for ( int i = 0; i < 2; i++ )
        {
            pooledChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

            pooledChannel.release().get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
        }

        notReleased.release().get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        assertEquals( 2, poolEventsMonitor.created );
        assertEquals( 1, poolEventsMonitor.acquired );
        assertEquals( 3, poolEventsMonitor.released );
    }

    @Test
    void shouldCreateNewChannelIfPreviousChannelFailedToConnect()
    {
        assertThrows( ExecutionException.class, () -> pool.acquire( serverlessAddress ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );
        assertThrows( ExecutionException.class, () -> pool.acquire( serverlessAddress ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );

        assertEquals( 2, poolEventsMonitor.created );
        assertEquals( 0, poolEventsMonitor.acquired );
        assertEquals( 0, poolEventsMonitor.released );
    }

    @Test
    void shouldBeAbleToSendToServer() throws InterruptedException, ExecutionException, TimeoutException
    {
        PooledChannel pooledChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        pooledChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> pooledChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
    }

    @Test
    void shouldFailToAcquireChannelIfNoServer()
    {
        assertThrows( ExecutionException.class, () -> pool.acquire( serverlessAddress ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );
    }

    @Test
    void shouldBeAbleToSendToServerAfterBeingRestarted() throws InterruptedException, ExecutionException, TimeoutException
    {
        PooledChannel preRestartChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        preRestartChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> preRestartChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        closeServers();

        startServers();

        PooledChannel postRestartChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        postRestartChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> postRestartChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
    }

    @Test
    void shouldReturnEmptyStreamOfInstalledProtocolsIfNoOpenChannels() throws ExecutionException, InterruptedException
    {
        // when
        Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols = pool.installedProtocols();

        pool.acquire( to1 ).get().channel().close().get();

        // then
        assertThat( installedProtocols, empty() );
    }

    @Test
    void shouldReturnEmptyStreamOfInstalledProtocolsIfChannelIsUnableToConnect()
    {
        assertThrows( Exception.class, () -> pool.acquire( serverlessAddress ).get().release().get() );
        // when
        Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols = pool.installedProtocols();

        // then
        assertThat( installedProtocols, empty() );
    }

    @Test
    void shouldReturnStreamOfInstalledProtocolsForChannelsThatHaveCompletedHandshake() throws ExecutionException, InterruptedException
    {
        pool.acquire( to1 ).get().release().get();
        pool.acquire( to2 ).get().release().get();

        List<Pair<SocketAddress,ProtocolStack>> installedProtocols = pool.installedProtocols().collect( toList() );

        assertThat( installedProtocols, Matchers.containsInAnyOrder( Pair.of( new SocketAddress( to1.getHostname(), to1.getPort() ), protocolStackRaft ),
                Pair.of( new SocketAddress( to2.getHostname(), to2.getPort() ), protocolStackRaft ) ) );
    }

    private void startServers() throws InterruptedException, ExecutionException
    {
        BootstrapConfiguration<? extends ServerSocketChannel> serverConfig = BootstrapConfiguration.serverConfig( Config.defaults() );
        serverEventExecutor = serverConfig.eventLoopGroup( Executors.newCachedThreadPool() );
        ServerBootstrap serverBootstrap = new ServerBootstrap().group( serverEventExecutor ).channel( serverConfig.channelClass() );

        ChannelFuture server1 = serverBootstrap.clone().childHandler( new EmptyChannelHandler() ).bind( 0 );
        ChannelFuture server2 = serverBootstrap.clone().childHandler( new EmptyChannelHandler() ).bind( 0 );

        server1.get();
        server2.get();

        AdvertisedSocketAddress server1Address = getLocalAddress( server1 );
        to1 = new AdvertisedSocketAddress( server1Address.getHostname(), server1Address.getPort() );
        AdvertisedSocketAddress server2Address = getLocalAddress( server2 );
        to2 = new AdvertisedSocketAddress( server2Address.getHostname(), server2Address.getPort() );
    }

    private AdvertisedSocketAddress getLocalAddress( ChannelFuture server1 )
    {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) server1.channel().localAddress();
        return new AdvertisedSocketAddress( inetSocketAddress.getHostName(), inetSocketAddress.getPort() );
    }

    private void closeServers()
    {
        serverEventExecutor.shutdownGracefully();
    }

    @ChannelHandler.Sharable
    private static class EmptyChannelHandler extends ChannelHandlerAdapter
    { }

    private ByteBuf emptyBuffer()
    {
        return ByteBufAllocator.DEFAULT.heapBuffer();
    }

    private class PoolEventsMonitor implements ChannelPoolHandler
    {
        private int created;
        private int acquired;
        private int released;

        @Override
        public void channelReleased( Channel ch )
        {
            released++;
        }

        @Override
        public void channelAcquired( Channel ch )
        {
            acquired++;
        }

        @Override
        public void channelCreated( Channel ch )
        {
            ch.attr( ChannelAttribute.PROTOCOL_STACK ).set( CompletableFuture.completedFuture( protocolStackRaft ) );
            created++;
        }
    }
}
