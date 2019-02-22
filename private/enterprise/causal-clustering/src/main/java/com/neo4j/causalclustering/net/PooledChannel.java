/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CompletableFuture;

public class PooledChannel
{
    private final Channel channel;
    private final ChannelPool pool;

    /**
     * Channel that belongs to a {@link ChannelPool}. Should always be released after finished using.
     *
     * @param channel the {@link Channel}
     * @param pool {@link ChannelPool} which the channel was acquired from.
     */
    PooledChannel( Channel channel, ChannelPool pool )
    {
        this.channel = channel;
        this.pool = pool;
    }

    static CompletableFuture<PooledChannel> future( Future<Channel> acquire, SimpleChannelPool channelPool )
    {
        CompletableFuture<PooledChannel> pooledChannelFuture = new CompletableFuture<>();
        acquire.addListener( (GenericFutureListener<Future<Channel>>) future ->
        {
            if ( future.isSuccess() )
            {
                pooledChannelFuture.complete( new PooledChannel( future.getNow(), channelPool ) );
            }
            else
            {
                if ( future.cause() != null )
                {
                    pooledChannelFuture.completeExceptionally( future.cause() );
                }
                else
                {
                    pooledChannelFuture.completeExceptionally( new IllegalStateException( "Failed to acquire channel from pool." ) );
                }
            }
        } );
        return pooledChannelFuture;
    }

    public <ATTR> ATTR getAttribute( AttributeKey<ATTR> key )
    {
        return channel.attr( key ).get();
    }

    public Channel channel()
    {
        return channel;
    }

    /**
     * Release the {@link Channel} back to the pool so that it can be reused. It's important to always do this after use to avoid the pool filling up.
     *
     * @return {@link Future} which is completed when the channel has been released.
     */
    public Future<Void> release()
    {
        return pool.release( channel );
    }
}
