/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;

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
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private CompletableFuture<PooledChannel> lifeCompletionStage;
    private SimpleChannelPoolMap poolMap;
    private EventLoopGroup eventLoopGroup;

    public ChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler scheduler, Group group,
            ChannelPoolHandler channelPoolHandler )
    {
        this.bootstrapConfiguration = bootstrapConfiguration;
        this.scheduler = scheduler;
        this.group = group;
        this.poolHandler = channelPoolHandler;
    }

    public CompletableFuture<PooledChannel> acquire( AdvertisedSocketAddress advertisedSocketAddress )
    {
        readWriteLock.readLock().lock();
        try
        {
            if ( poolMap == null )
            {
                return failedFuture( new IllegalStateException( "Channel pool service is not in a started state." ) );
            }
            SimpleChannelPool channelPool = poolMap.get( advertisedSocketAddress );
            return PooledChannel.future( channelPool.acquire(), channelPool ).applyToEither( lifeCompletionStage, pooledChannel -> pooledChannel );
        }
        finally
        {
            readWriteLock.readLock().unlock();
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
        readWriteLock.writeLock().lock();
        try
        {
            lifeCompletionStage = new CompletableFuture<>();
            eventLoopGroup = bootstrapConfiguration.eventLoopGroup( scheduler.executor( group ) );
            Bootstrap baseBootstrap = new Bootstrap().group( eventLoopGroup ).channel( bootstrapConfiguration.channelClass() );
            poolMap = new SimpleChannelPoolMap( baseBootstrap, poolHandler );
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void stop()
    {
        readWriteLock.writeLock().lock();
        try
        {
            lifeCompletionStage.completeExceptionally( new IllegalStateException( "Lifecycle has stopped" ) );
            if ( poolMap != null )
            {
                poolMap.close();
            }
            poolMap = null;
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void shutdown()
    {
        // do nothing
    }

    public Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        readWriteLock.readLock().lock();
        try
        {
            return poolMap == null ? Stream.empty() : poolMap.installedProtocols();
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
    }
}
