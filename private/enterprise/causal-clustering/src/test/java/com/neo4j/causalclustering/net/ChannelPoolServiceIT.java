/*
 * Copyright (c) "Neo4j"
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
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.ServerSocketChannel;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.scheduler.Group;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static co.unruly.matchers.StreamMatchers.empty;
import static com.neo4j.causalclustering.net.ChannelPoolService.SOCKET_TO_INET;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.function.ThrowingAction.executeAll;

class ChannelPoolServiceIT
{
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    private static final int DEFAULT_TIME_OUT = 30;
    private final ProtocolStack protocolStackRaft = new ProtocolStack( TestApplicationProtocols.RAFT_2, emptyList() );
    private ChannelPoolService<SocketAddress> pool;
    private SocketAddress to1;
    private SocketAddress to2;
    private final SocketAddress serverlessAddress = new SocketAddress( "localhost", PortAuthority.allocatePort() );
    private ExecutorService executor;
    private EventLoopGroup serverEventExecutor;
    private PoolEventsMonitor poolEventsMonitor;
    private ThreadPoolJobScheduler poolScheduler;

    @BeforeEach
    void setUpServers() throws Exception
    {
        poolEventsMonitor = new PoolEventsMonitor();
        poolScheduler = new ThreadPoolJobScheduler();
        pool = new ChannelPoolService<>( BootstrapConfiguration.clientConfig( Config.defaults() ), poolScheduler, Group.RAFT_CLIENT,
                                         poolEventsMonitor, SimpleChannelPool::new, SOCKET_TO_INET, TrackingChannelPoolMap::new );

        startServers();

        pool.start();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        executeAll( this::closeServers, pool::stop, poolScheduler::shutdown );
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

        assertEquals( 1, poolEventsMonitor.created() );
        assertEquals( 2, poolEventsMonitor.acquired() );
        assertEquals( 2, poolEventsMonitor.released() );
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

        assertEquals( 2, poolEventsMonitor.created() );
        assertEquals( 3, poolEventsMonitor.acquired() );
        assertEquals( 3, poolEventsMonitor.released() );
    }

    @Test
    void shouldCreateNewChannelIfPreviousChannelFailedToConnect()
    {
        assertThrows( ExecutionException.class, () -> pool.acquire( serverlessAddress ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );
        assertThrows( ExecutionException.class, () -> pool.acquire( serverlessAddress ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );

        assertEquals( 2, poolEventsMonitor.created() );
        assertEquals( 0, poolEventsMonitor.acquired() );
        assertEquals( 0, poolEventsMonitor.released() );
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
    void shouldBeAbleToSendToServerAfterServerBeingRestarted() throws Exception
    {
        PooledChannel preRestartChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        preRestartChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> preRestartChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        closeServers();

        startServers();

        PooledChannel postRestartChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        postRestartChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> postRestartChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
    }

    @Test
    void shouldBeAbleToSendToServerAfterPoolBeingRestarted() throws Exception
    {
        PooledChannel preRestartChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        preRestartChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> preRestartChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        pool.stop();
        pool.start();

        PooledChannel postRestartChannel = pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );

        postRestartChannel.channel().writeAndFlush( emptyBuffer() ).addListener( f -> postRestartChannel.release() ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT );
    }

    @Test
    void shouldFailExceptionallyIfPoolIsStopped()
    {
        pool.stop();
        ExecutionException executionException = assertThrows( ExecutionException.class, () -> pool.acquire( to1 ).get( DEFAULT_TIME_OUT, DEFAULT_TIME_UNIT ) );

        IllegalStateException cause = (IllegalStateException) executionException.getCause();
        assertEquals( "Channel pool service is not in a started state.", cause.getMessage() );
    }

    @Test
    void shouldReturnEmptyStreamOfInstalledProtocolsIfNoOpenChannels() throws Exception
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
    void shouldReturnStreamOfInstalledProtocolsForChannelsThatHaveCompletedHandshake() throws Exception
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
        executor = Executors.newCachedThreadPool();
        serverEventExecutor = serverConfig.eventLoopGroup( executor );
        ServerBootstrap serverBootstrap = new ServerBootstrap().group( serverEventExecutor ).channel( serverConfig.channelClass() );

        ChannelFuture server1 = serverBootstrap.clone().childHandler( new EmptyChannelHandler() ).bind( 0 );
        ChannelFuture server2 = serverBootstrap.clone().childHandler( new EmptyChannelHandler() ).bind( 0 );

        server1.get();
        server2.get();

        SocketAddress server1Address = getLocalAddress( server1 );
        to1 = new SocketAddress( server1Address.getHostname(), server1Address.getPort() );
        SocketAddress server2Address = getLocalAddress( server2 );
        to2 = new SocketAddress( server2Address.getHostname(), server2Address.getPort() );
    }

    private SocketAddress getLocalAddress( ChannelFuture server1 )
    {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) server1.channel().localAddress();
        return new SocketAddress( inetSocketAddress.getHostName(), inetSocketAddress.getPort() );
    }

    private void closeServers() throws Exception
    {
        executeAll(
                () -> serverEventExecutor.shutdownGracefully().get(),
                () ->
                {
                    executor.shutdownNow();
                    assertTrue( executor.awaitTermination( 30, DEFAULT_TIME_UNIT ) );
                } );
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
        private AtomicInteger created = new AtomicInteger();
        private AtomicInteger acquired = new AtomicInteger();
        private AtomicInteger released = new AtomicInteger();

        @Override
        public void channelReleased( Channel ch )
        {
            released.getAndIncrement();
        }

        @Override
        public void channelAcquired( Channel ch )
        {
            acquired.getAndIncrement();
        }

        @Override
        public void channelCreated( Channel ch )
        {
            ch.attr( ChannelAttribute.PROTOCOL_STACK ).set( CompletableFuture.completedFuture( protocolStackRaft ) );
            created.getAndIncrement();
        }

        int created()
        {
            return created.get();
        }

        int acquired()
        {
            return acquired.get();
        }

        int released()
        {
            return released.get();
        }
    }
}
