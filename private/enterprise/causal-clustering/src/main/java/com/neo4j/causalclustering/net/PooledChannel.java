/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

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
