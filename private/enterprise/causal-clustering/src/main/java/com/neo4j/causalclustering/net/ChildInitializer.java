/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public interface ChildInitializer
{
    void initChannel( Channel channel ) throws Exception;

    default ChannelInitializer<Channel> asChannelInitializer()
    {
        return new ChannelInitializer<>()
        {
            @Override
            protected void initChannel( Channel channel ) throws Exception
            {
                ChildInitializer.this.initChannel( channel );
            }
        };
    }
}
