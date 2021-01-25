/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;

import java.util.concurrent.Executor;

public abstract class EpollBootstrapConfig<CHANNEL extends Channel> implements BootstrapConfiguration<CHANNEL>
{
    public static EpollBootstrapConfig<EpollServerSocketChannel> epollServerConfig()
    {
        return new EpollBootstrapConfig<>()
        {
            @Override
            public Class<EpollServerSocketChannel> channelClass()
            {
                return EpollServerSocketChannel.class;
            }
        };
    }

    public static EpollBootstrapConfig<EpollSocketChannel> epollClientConfig()
    {
        return new EpollBootstrapConfig<>()
        {
            @Override
            public Class<EpollSocketChannel> channelClass()
            {
                return EpollSocketChannel.class;
            }
        };
    }

    @Override
    public EventLoopGroup eventLoopGroup( Executor executor )
    {
        return new EpollEventLoopGroup( 0, executor );
    }
}
