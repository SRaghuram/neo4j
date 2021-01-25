/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;

import java.util.concurrent.Executor;

public abstract class KQueueBootstrapConfig<CHANNEL extends Channel> implements BootstrapConfiguration<CHANNEL>
{
    public static KQueueBootstrapConfig<KQueueServerSocketChannel> kQueueServerConfig()
    {
        return new KQueueBootstrapConfig<>()
        {
            @Override
            public Class<KQueueServerSocketChannel> channelClass()
            {
                return KQueueServerSocketChannel.class;
            }
        };
    }

    public static KQueueBootstrapConfig<KQueueSocketChannel> kQueueClientConfig()
    {
        return new KQueueBootstrapConfig<>()
        {
            @Override
            public Class<KQueueSocketChannel> channelClass()
            {
                return KQueueSocketChannel.class;
            }
        };
    }

    @Override
    public EventLoopGroup eventLoopGroup( Executor executor )
    {
        return new KQueueEventLoopGroup( 0, executor );
    }
}
