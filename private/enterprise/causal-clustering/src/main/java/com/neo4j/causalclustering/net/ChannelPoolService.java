/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.util.concurrent.Futures.failedFuture;

public class ChannelPoolService implements Lifecycle
{
    private final BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration;
    private final JobScheduler scheduler;
    private final Group group;
    private final ChannelPoolHandler poolHandler;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock exclusiveService = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock sharedService = lock.readLock();
    private CompletableFuture<PooledChannel> endOfLife;

    private SimpleChannelPoolMap poolMap; // used as "is stopped" flag, stopped when null
    private EventLoopGroup eventLoopGroup;

    public ChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler scheduler, Group group,
            ChannelPoolHandler channelPoolHandler )
    {
        this.bootstrapConfiguration = bootstrapConfiguration;
        this.scheduler = scheduler;
        this.group = group;
        this.poolHandler = channelPoolHandler;
    }

    public CompletableFuture<PooledChannel> acquire( AdvertisedSocketAddress address )
    {
        sharedService.lock();
        try
        {
            if ( poolMap == null )
            {
                return failedFuture( new IllegalStateException( "Channel pool service is not in a started state." ) );
            }
            return acquire0( address ).applyToEither( endOfLife, pooledChannel -> pooledChannel );
        }
        finally
        {
            sharedService.unlock();
        }
    }

    private CompletableFuture<PooledChannel> acquire0( AdvertisedSocketAddress address )
    {
        SimpleChannelPool pool = poolMap.get( address );
        Future<Channel> fChannel = pool.acquire();

        CompletableFuture<PooledChannel> pooledChannelFuture = new CompletableFuture<>();
        fChannel.addListener( (GenericFutureListener<Future<Channel>>) f ->
        {
            if ( f.isSuccess() )
            {
                pooledChannelFuture.complete( new PooledChannel( f.get(), pool ) );
            }
            else
            {
                pooledChannelFuture.completeExceptionally( f.cause() );
            }
        } );
        return pooledChannelFuture;
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
            poolMap = new SimpleChannelPoolMap( baseBootstrap, poolHandler );
        }
        finally
        {
            exclusiveService.unlock();
        }
    }

    @Override
    public void stop()
    {
        sharedService.lock();
        // usages of the pool should have been stopped before this point, hence illegal state exception
        endOfLife.completeExceptionally( new IllegalStateException( "Pool is closed. Lifecycle issue?" ) );
        sharedService.unlock();

        exclusiveService.lock();
        try
        {
            if ( poolMap != null )
            {
                poolMap.close();
                poolMap = null;
            }
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
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
}
