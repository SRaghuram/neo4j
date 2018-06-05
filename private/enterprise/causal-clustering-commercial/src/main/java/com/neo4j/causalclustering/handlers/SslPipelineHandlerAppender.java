/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.ssl.SslPolicy;

public class SslPipelineHandlerAppender implements PipelineHandlerAppender
{
    private final SslPolicy sslPolicy;

    public SslPipelineHandlerAppender( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }

    @Override
    public void addPipelineHandlerForServer( ChannelPipeline pipeline, Channel ch ) throws Exception
    {
        if ( sslPolicy != null )
        {
            pipeline.addLast( sslPolicy.nettyServerHandler( ch ) );
        }
    }

    @Override
    public void addPipelineHandlerForClient( ChannelPipeline pipeline, Channel ch ) throws Exception
    {
        if ( sslPolicy != null )
        {
            pipeline.addLast( sslPolicy.nettyClientHandler( ch ) );
        }
    }
}
