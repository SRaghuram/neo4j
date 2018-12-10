/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import com.neo4j.causalclustering.handlers.PipelineWrapper;
import com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import org.neo4j.logging.Log;

public class NettyPipelineBuilderFactory
{
    private final PipelineWrapper wrapper;

    public NettyPipelineBuilderFactory( PipelineWrapper wrapper )
    {
        this.wrapper = wrapper;
    }

    public ClientNettyPipelineBuilder client( Channel channel, Log log ) throws Exception
    {
        return create( channel, NettyPipelineBuilder.client( channel.pipeline(), log ) );
    }

    public ServerNettyPipelineBuilder server( Channel channel, Log log ) throws Exception
    {
        return create( channel, NettyPipelineBuilder.server( channel.pipeline(), log ) );
    }

    private <O extends Orientation, BUILDER extends NettyPipelineBuilder<O,BUILDER>> BUILDER create(
            Channel channel, BUILDER nettyPipelineBuilder ) throws Exception
    {
        int i = 0;
        for ( ChannelHandler handler : wrapper.handlersFor( channel ) )
        {
            nettyPipelineBuilder.add( String.format( "%s_%d", wrapper.name(), i ), handler );
            i++;
        }
        return nettyPipelineBuilder;
    }
}
