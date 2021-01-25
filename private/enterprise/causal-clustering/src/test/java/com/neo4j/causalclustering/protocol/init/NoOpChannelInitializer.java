/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class NoOpChannelInitializer extends ChannelInitializer<Channel>
{
    private volatile boolean invoked;

    @Override
    protected void initChannel( Channel ch )
    {
        invoked = true;
    }

    public boolean invoked()
    {
        return invoked;
    }
}
