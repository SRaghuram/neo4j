/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.helper.ErrorHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.resolver.AbstractAddressResolver;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.causalclustering.net.ChannelPoolService.SOCKET_TO_INET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrackingChannelPoolMapTest
{
    private AtomicInteger resolved = new AtomicInteger();
    private int port = PortAuthority.allocatePort();

    @ParameterizedTest
    @EnumSource( IpFamily.class )
    void shouldAlwaysResolveAddress( IpFamily ipFamily ) throws InterruptedException
    {
        BootstrapConfiguration<? extends SocketChannel> clientConfig = BootstrapConfiguration.clientConfig( Config.defaults() );
        var executorService = Executors.newCachedThreadPool();
        var clientGroup = clientConfig.eventLoopGroup( executorService );
        BootstrapConfiguration<? extends ServerSocketChannel> serverConfig = BootstrapConfiguration.serverConfig( Config.defaults() );
        var serverGroup = serverConfig.eventLoopGroup( executorService );

        try
        {
            TrackingChannelPoolMap cpm = createPoolMap( clientConfig, clientGroup );

            startEmptyServer( serverConfig, serverGroup );

            // create a pool for the specific address
            var channelPool = cpm.newPool( new SocketAddress( ipFamily.localhostName(), port ) );
            Future<Channel> channel1 = channelPool.acquire().sync();
            Future<Channel> channel2 = channelPool.acquire().sync();

            assertTrue( channel1.isSuccess() );
            assertTrue( channel2.isSuccess() );
            assertEquals( 2, resolved.get() );
        }
        finally
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Shutdown executors" ) )
            {
                errorHandler.execute( executorService::shutdown );
                errorHandler.execute( () -> clientGroup.shutdownGracefully().sync() );
                errorHandler.execute( () -> serverGroup.shutdownGracefully().sync() );
            }
        }
    }

    private void startEmptyServer( BootstrapConfiguration<? extends ServerSocketChannel> serverConfig, EventLoopGroup serverGroup ) throws InterruptedException
    {
        new ServerBootstrap().childHandler( new EmptyChannelHandler() ).group( serverGroup ).channel( serverConfig.channelClass() ).bind( port ).sync();
    }

    private TrackingChannelPoolMap<SocketAddress> createPoolMap( BootstrapConfiguration<? extends SocketChannel> clientConfig, EventLoopGroup clientGroup )
    {
        var bootstrap = new Bootstrap().group( clientGroup ).channel( clientConfig.channelClass() ).resolver( new AddressResolverGroup<InetSocketAddress>()
        {
            @Override
            protected AddressResolver<InetSocketAddress> newResolver( EventExecutor executor )
            {
                var resolver = DefaultAddressResolverGroup.INSTANCE.getResolver( executor );
                return new AlwaysResolve( executor, resolver );
            }
        } );
        return new TrackingChannelPoolMap<>( bootstrap, new AbstractChannelPoolHandler()
        {
            @Override
            public void channelCreated( Channel ch )
            {

            }
        }, SimpleChannelPool::new, SOCKET_TO_INET );
    }

    class AlwaysResolve extends AbstractAddressResolver<InetSocketAddress>
    {

        private final AddressResolver<InetSocketAddress> resolver;

        AlwaysResolve( EventExecutor executor, AddressResolver<InetSocketAddress> resolver )
        {
            super( executor );
            this.resolver = resolver;
        }

        @Override
        protected boolean doIsResolved( InetSocketAddress address )
        {
            assertFalse( resolver.isResolved( address ), "Address should never be resolved" );
            return false;
        }

        @Override
        protected void doResolve( InetSocketAddress unresolvedAddress, Promise<InetSocketAddress> promise )
        {
            resolved.incrementAndGet();
            resolver.resolve( unresolvedAddress, promise );
        }

        @Override
        protected void doResolveAll( InetSocketAddress unresolvedAddress, Promise<List<InetSocketAddress>> promise )
        {
            resolver.resolveAll( unresolvedAddress, promise );
        }
    }

    @ChannelHandler.Sharable
    private static class EmptyChannelHandler extends ChannelHandlerAdapter
    { }
}
