/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.net.TrackingChannelPoolMap.TrackingChannelPoolMapFactory;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.net.NettyUtil.toCompletableFuture;
import static com.neo4j.causalclustering.protocol.handshake.ChannelAttribute.PROTOCOL_STACK;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class ChannelPoolService<T> implements Lifecycle
{

    protected static final Function<SocketAddress,InetSocketAddress> SOCKET_TO_INET =
            socketAddress -> InetSocketAddress.createUnresolved( socketAddress.getHostname(), socketAddress.getPort() );

    private final BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration;
    private final JobScheduler scheduler;
    private final Group group;
    private final ChannelPoolHandler poolHandler;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock exclusiveService = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock sharedService = lock.readLock();
    private CompletableFuture<PooledChannel> endOfLife;

    private TrackingChannelPoolMap<T> poolMap; // used as "is stopped" flag, stopped when null
    private final ChannelPoolFactory poolFactory;
    private final Function<T,InetSocketAddress> keyToInetAddress;
    private final TrackingChannelPoolMapFactory<T> poolMapFactory;
    private EventLoopGroup eventLoopGroup;

    public ChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration,
                               JobScheduler scheduler,
                               Group group,
                               ChannelPoolHandler channelPoolHandler,
                               ChannelPoolFactory poolFactory,
                               Function<T,InetSocketAddress> keyToInetAddress,
                               TrackingChannelPoolMapFactory<T> poolMapFactory
    )
    {
        this.bootstrapConfiguration = bootstrapConfiguration;
        this.scheduler = scheduler;
        this.group = group;
        this.poolHandler = channelPoolHandler;
        this.poolFactory = poolFactory;
        this.keyToInetAddress = keyToInetAddress;
        this.poolMapFactory = poolMapFactory;
    }

    public CompletableFuture<PooledChannel> acquire( T address )
    {
        sharedService.lock();
        try
        {
            if ( poolMap == null )
            {
                return failedFuture( new IllegalStateException( "Channel pool service is not in a started state." ) );
            }

            ChannelPool pool = poolMap.get( address );
            return toCompletableFuture( pool.acquire() )
                    .thenCompose( channel -> createPooledChannel( channel, pool ) )
                    .applyToEither( endOfLife, Function.identity() );
        }
        finally
        {
            sharedService.unlock();
        }
    }

    @Override
    public void init()
    {
        // do nothing
    }

    @Override
    public void start()
    {
        exclusiveService.lock();
        try
        {
            endOfLife = new CompletableFuture<>();
            eventLoopGroup = bootstrapConfiguration.eventLoopGroup( scheduler.executor( group ) );
            Bootstrap baseBootstrap = new Bootstrap().group( eventLoopGroup ).channel( bootstrapConfiguration.channelClass() );
            poolMap = poolMapFactory.create( baseBootstrap, poolHandler, poolFactory, keyToInetAddress );
        }
        finally
        {
            exclusiveService.unlock();
        }
    }

    @Override
    public void stop()
    {
        // usages of the pool should have been stopped before this point, hence illegal state exception
        endOfLife.completeExceptionally( new IllegalStateException( "Pool is closed. Lifecycle issue?" ) );

        exclusiveService.lock();
        try
        {
            if ( poolMap != null )
            {
                poolMap.close();
                poolMap = null;
            }
            // A quiet period of exactly zero cannot be used because that won't finish all queued tasks,
            // which is the guarantee we want, because we don't care about a quiet period per se.
            eventLoopGroup.shutdownGracefully( 100, 5000, TimeUnit.MILLISECONDS ).syncUninterruptibly();
        }
        finally
        {
            exclusiveService.unlock();
        }
    }

    @Override
    public void shutdown()
    {
        // do nothing
    }

    public Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        sharedService.lock();
        try
        {
            return poolMap == null ? Stream.empty() : poolMap.installedProtocols();
        }
        finally
        {
            sharedService.unlock();
        }
    }

    private static CompletionStage<PooledChannel> createPooledChannel( Channel channel, ChannelPool pool )
    {
        var protocolFuture = channel.attr( PROTOCOL_STACK ).get();
        if ( protocolFuture == null )
        {
            return failedFuture( new IllegalStateException( "Channel " + channel + " does not contain a protocol stack attribute" ) );
        }
        return protocolFuture.thenApply( protocol -> new PooledChannel( channel, pool, protocol ) );
    }
}
