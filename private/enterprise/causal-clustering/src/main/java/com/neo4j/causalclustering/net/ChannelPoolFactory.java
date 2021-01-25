/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;

public interface ChannelPoolFactory
{
    ChannelPool create( Bootstrap bootstrap, ChannelPoolHandler handler );
}
