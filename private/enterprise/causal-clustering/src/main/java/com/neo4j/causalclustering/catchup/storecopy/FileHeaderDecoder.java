/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.string.UTF8;

public class FileHeaderDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out )
    {
        int length = msg.readInt();
        byte[] bytes = new byte[length];
        msg.readBytes( bytes );
        String name = UTF8.decode( bytes );
        int requiredAlignment = msg.readInt();
        out.add( new FileHeader( name, requiredAlignment ) );
    }
}
