/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

public class PooledChannel
{
    private final Channel channel;
    private final ChannelPool pool;
    private final ProtocolStack protocolStack;

    /**
     * Channel that belongs to a {@link ChannelPool}. Should always be released after finished using.
     *
     * @param channel the {@link Channel}
     * @param pool {@link ChannelPool} which the channel was acquired from.
     * @param protocolStack the protocol stack used by this channel.
     */
    PooledChannel( Channel channel, ChannelPool pool, ProtocolStack protocolStack )
    {
        this.channel = channel;
        this.pool = pool;
        this.protocolStack = protocolStack;
    }

    public <ATTR> ATTR getAttribute( AttributeKey<ATTR> key )
    {
        return channel.attr( key ).get();
    }

    public Channel channel()
    {
        return channel;
    }

    public ProtocolStack protocolStack()
    {
        return protocolStack;
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

    /**
     * Disposes the {@link Channel} so that it cannot be reused.
     *
     * @return {@link Future} which is completed when the channel has been disposed.
     */
    public Future<Void> dispose()
    {
        channel.close();
        return pool.release( channel );
    }
}
