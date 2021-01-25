/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import io.netty.channel.Channel;

import org.neo4j.logging.Log;
import org.neo4j.ssl.SslPolicy;

public class NettyPipelineBuilderFactory
{
    private final SslPolicy sslPolicy;

    public NettyPipelineBuilderFactory( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }

    public static NettyPipelineBuilderFactory insecure()
    {
        return new NettyPipelineBuilderFactory( null );
    }

    public ClientNettyPipelineBuilder client( Channel channel, Log log )
    {
        return NettyPipelineBuilder.client( channel.pipeline(), sslPolicy, log );
    }

    public ServerNettyPipelineBuilder server( Channel channel, Log log )
    {
        return NettyPipelineBuilder.server( channel.pipeline(), sslPolicy, log );
    }
}
