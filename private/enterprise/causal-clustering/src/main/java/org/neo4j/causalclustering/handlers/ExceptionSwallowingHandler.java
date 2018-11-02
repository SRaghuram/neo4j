/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.handlers;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class ExceptionSwallowingHandler extends ChannelHandlerAdapter
{
    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        // yummy
    }
}
