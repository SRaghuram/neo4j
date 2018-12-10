/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public interface ChildInitializer
{
    void initChannel( SocketChannel channel ) throws Exception;

    default ChannelInitializer<SocketChannel> asChannelInitializer()
    {
        return new ChannelInitializer<SocketChannel>()
        {
            @Override
            protected void initChannel( SocketChannel channel ) throws Exception
            {
                ChildInitializer.this.initChannel( channel );
            }
        };
    }
}
