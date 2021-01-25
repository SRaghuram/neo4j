/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.AbstractNioChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.Executor;

public abstract class NioBootstrapConfig<CHANNEL extends AbstractNioChannel> implements BootstrapConfiguration<CHANNEL>
{
    public static NioBootstrapConfig<NioServerSocketChannel> nioServerConfig()
    {
        return new NioBootstrapConfig<>()
        {
            @Override
            public Class<NioServerSocketChannel> channelClass()
            {
                return NioServerSocketChannel.class;
            }
        };
    }

    public static NioBootstrapConfig<NioSocketChannel> nioClientConfig()
    {
        return new NioBootstrapConfig<>()
        {
            @Override
            public Class<NioSocketChannel> channelClass()
            {
                return NioSocketChannel.class;
            }
        };
    }

    @Override
    public EventLoopGroup eventLoopGroup( Executor executor )
    {
        return new NioEventLoopGroup( 0, executor );
    }
}
