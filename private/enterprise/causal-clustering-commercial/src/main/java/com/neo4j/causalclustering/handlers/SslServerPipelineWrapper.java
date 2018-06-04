/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import java.util.List;

import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.ssl.SslPolicy;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class SslServerPipelineWrapper implements PipelineWrapper
{
    private final SslPolicy sslPolicy;

    SslServerPipelineWrapper( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }

    @Override
    public List<ChannelHandler> handlersFor( Channel channel ) throws Exception
    {
        if ( sslPolicy != null )
        {
            return singletonList( sslPolicy.nettyServerHandler( channel ) );
        }
        else
        {
            return emptyList();
        }
    }

    @Override
    public String name()
    {
        return "ssl_server";
    }
}
