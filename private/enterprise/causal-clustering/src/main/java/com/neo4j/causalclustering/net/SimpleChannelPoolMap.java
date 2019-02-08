/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;

import org.neo4j.helpers.AdvertisedSocketAddress;

class SimpleChannelPoolMap extends AbstractChannelPoolMap<AdvertisedSocketAddress,SimpleChannelPool>
{
    private final Bootstrap baseBootstrap;
    private final ChannelPoolHandler poolHandler;

    SimpleChannelPoolMap( Bootstrap baseBootstrap, ChannelPoolHandler poolHandler )
    {
        this.baseBootstrap = baseBootstrap;
        this.poolHandler = poolHandler;
    }

    @Override
    protected SimpleChannelPool newPool( AdvertisedSocketAddress key )
    {
        return new SimpleChannelPool( baseBootstrap.remoteAddress( key.socketAddress() ), poolHandler );
    }
}
