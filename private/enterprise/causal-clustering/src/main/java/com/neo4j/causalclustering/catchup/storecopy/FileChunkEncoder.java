/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.HEADER_SIZE;
import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.makeHeader;

public class FileChunkEncoder extends MessageToMessageEncoder<FileChunk>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, FileChunk chunk, List<Object> out )
    {
        ByteBuf header = ctx.alloc().ioBuffer( HEADER_SIZE );
        header.writeInt( makeHeader( chunk.isLast() ) );

        ByteBuf payload = chunk.payload();

        CompositeByteBuf frame = ctx.alloc().compositeBuffer( 2 );

        frame.addComponent( true, header );
        frame.addComponent( true, payload );

        out.add( frame );
    }
}
