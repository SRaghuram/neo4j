/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import static com.neo4j.causalclustering.net.NettyUtil.chain;

/**
 * Returns the same channel to every client, without requiring the previous client to have released it.
 */
public class OneMultiplexedChannel extends SimpleChannelPool
{
    private volatile Future<Channel> fChannel;

    public OneMultiplexedChannel( Bootstrap bootstrap, ChannelPoolHandler handler )
    {
        super( bootstrap, handler );
    }

    @Override
    public Future<Channel> acquire( Promise<Channel> promise )
    {
        Future<Channel> fChannelAlias = fChannel;
        if ( fChannelAlias != null && fChannelAlias.isSuccess() )
        {
            Channel channel = fChannelAlias.getNow();
            if ( channel != null && channel.isActive() )
            {
                promise.trySuccess( channel );
                return promise;
            }
        }

        return acquireSync( promise );
    }

    private synchronized Future<Channel> acquireSync( Promise<Channel> promise )
    {
        if ( fChannel == null )
        {
            return fChannel = super.acquire( promise );
        }
        else if ( !fChannel.isDone() )
        {
            return chain( fChannel, promise );
        }

        Channel channel = fChannel.getNow();
        if ( channel == null || !channel.isActive() )
        {
            return fChannel = super.acquire( promise );
        }
        else
        {
            promise.trySuccess( channel );
            return promise;
        }
    }

    @Override
    public Future<Void> release( Channel channel, Promise<Void> promise )
    {
        promise.trySuccess( null );
        return promise;
    }

    @Override
    public void close()
    {
        super.close(); // expected to do nothing, since we never release to it
        closeAwait( fChannel );
    }

    private static void closeAwait( Future<Channel> fChannel )
    {
        if ( fChannel == null )
        {
            return;
        }

        fChannel.awaitUninterruptibly();

        Channel channel = fChannel.getNow();
        if ( channel == null )
        {
            return;
        }

        channel.close().awaitUninterruptibly();
    }
}
