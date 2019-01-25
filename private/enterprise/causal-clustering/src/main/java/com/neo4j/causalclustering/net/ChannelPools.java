/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.protocol.handshake.ChannelAttribute;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class ChannelPools implements Lifecycle
{
    private final ConcurrentHashMap<AdvertisedSocketAddress,SimpleChannelPool> poolMap = new ConcurrentHashMap<>();
    private final BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration;
    private final JobScheduler scheduler;
    private final ChannelPoolHandler poolHandler;
    private Bootstrap baseBootstrap;
    private volatile boolean running;
    private EventLoopGroup eventLoopGroup;

    public ChannelPools( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler scheduler, ChannelPoolHandler channelPoolHandler )
    {
        this.bootstrapConfiguration = bootstrapConfiguration;
        this.scheduler = scheduler;
        this.poolHandler = channelPoolHandler;
    }

    public CompletableFuture<PooledChannel> acquire( AdvertisedSocketAddress advertisedSocketAddress )
    {
        if ( !running )
        {
            return null;
        }
        SimpleChannelPool channelPool = poolMap.computeIfAbsent( advertisedSocketAddress,
                advertisedSocketAddress1 -> new SimpleChannelPool( baseBootstrap.remoteAddress( advertisedSocketAddress1.socketAddress() ), poolHandler ) );
        return PooledChannel.future( channelPool.acquire(), channelPool );
    }

    @Override
    public void init()
    {
        // do nothing
    }

    @Override
    public void start()
    {
        eventLoopGroup = bootstrapConfiguration.eventLoopGroup( scheduler.executor( Group.RAFT_CLIENT ) );
        baseBootstrap = new Bootstrap().group( eventLoopGroup ).channel( bootstrapConfiguration.channelClass() );
        running = true;
    }

    @Override
    public void stop()
    {
        running = false;
        for ( SimpleChannelPool value : poolMap.values() )
        {
            value.close();
        }
        poolMap.clear();
        eventLoopGroup.shutdownGracefully().syncUninterruptibly();
    }

    @Override
    public void shutdown()
    {
        // do nothing
    }

    public Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols()
    {
        return poolMap.entrySet().stream().map( e -> Pair.of( e.getKey(), protocolStack( e.getValue() ) ) ).filter( p -> p.other() != null );
    }

    private ProtocolStack protocolStack( ChannelPool pool )
    {
        Channel channel = null;
        try
        {
            channel = pool.acquire().get( 100, TimeUnit.MILLISECONDS );
            return channel.attr( ChannelAttribute.PROTOCOL_STACK ).get().getNow( null );
        }
        catch ( InterruptedException | ExecutionException | TimeoutException ignore )
        {
        }
        finally
        {
            if ( channel != null )
            {
                pool.release( channel );
            }
        }
        return null;
    }
}
