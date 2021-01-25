/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.string.UTF8;

public class FileHeaderEncoder extends MessageToByteEncoder<FileHeader>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, FileHeader msg, ByteBuf out )
    {
        String name = msg.fileName();
        byte[] bytes = UTF8.encode( name );
        out.writeInt( bytes.length );
        out.writeBytes( bytes );
        out.writeInt( msg.requiredAlignment() );
    }
}
