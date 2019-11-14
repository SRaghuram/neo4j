/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

/**
 * Returns the same channel to every client, without requiring the previous client to have released it.
 */
public class OneMultiplexedChannel extends SimpleChannelPool
{
    private volatile Future<Channel> fChannel;

    OneMultiplexedChannel( Bootstrap bootstrap, ChannelPoolHandler handler )
    {
        super( bootstrap, handler );
    }

    @Override
    public Future<Channel> acquire( Promise<Channel> promise )
    {
        Future<Channel> fChannelAlias = fChannel;
        if ( fChannelAlias != null && (!fChannelAlias.isDone() || fChannelAlias.getNow().isOpen()) )
        {
            // fast path to return a possibly still good channel
            return fChannelAlias;
        }

        synchronized ( this )
        {
            if ( fChannel == null )
            {
                return fChannel = super.acquire( promise );
            }
            else if ( !fChannel.isDone() )
            {
                return fChannel;
            }

            Channel channel = fChannel.getNow();
            if ( channel.isOpen() )
            {
                return fChannel;
            }
            else
            {
                channel.close();
                return fChannel = super.acquire( promise );
            }
        }
    }

    @Override
    public Future<Void> release( Channel channel, Promise<Void> promise )
    {
        promise.trySuccess( null );
        return promise;
    }
}
