/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class ExceptionMonitoringHandler  extends ChannelHandlerAdapter
{
    public interface Monitor
    {
        void exceptionCaught( Channel channel, Throwable cause );
    }

    private final Monitor monitor;

    public ExceptionMonitoringHandler( Monitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        if ( ctx != null )
        {
            monitor.exceptionCaught( ctx.channel(), cause );
            ctx.fireExceptionCaught( cause );
        }
        else
        {
            monitor.exceptionCaught( null, cause );
        }
    }
}
