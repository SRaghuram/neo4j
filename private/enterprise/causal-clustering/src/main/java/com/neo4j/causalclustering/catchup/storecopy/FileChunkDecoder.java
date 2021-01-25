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

import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.parseHeader;

public class FileChunkDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf frame, List<Object> out )
    {
        int header = frame.readInt();
        boolean isLast = parseHeader( header );
        ByteBuf payload = frame.readRetainedSlice( frame.readableBytes() );

        boolean success = false;
        try
        {
            FileChunk fileChunk = new FileChunk( isLast, payload );
            out.add( fileChunk );
            success = true;
        }
        finally
        {
            if ( !success )
            {
                payload.release();
            }
        }
    }
}
