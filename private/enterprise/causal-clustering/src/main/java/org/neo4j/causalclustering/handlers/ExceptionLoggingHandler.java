/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.handlers;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import org.neo4j.logging.Log;

import static java.lang.String.format;

public class ExceptionLoggingHandler extends ChannelHandlerAdapter
{
    private final Log log;

    public ExceptionLoggingHandler( Log log )
    {
        this.log = log;
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        if ( ctx != null )
        {
            log.error( format( "Failed to process message on channel %s.", ctx.channel() ), cause );
            ctx.fireExceptionCaught( cause );
        }
        else
        {
            log.error( "Failed to process message on a null channel.", cause );
        }
    }
}
