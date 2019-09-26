/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_PAYLOAD_SIZE;
import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS;

public class FileChunkDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out )
    {
        int encodedLength = msg.readInt();
        int length = encodedLength == USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS ? MAX_PAYLOAD_SIZE : encodedLength;

        ByteBuf payload = msg.readRetainedSlice( length );

        boolean success = false;
        try
        {
            FileChunk fileChunk = new FileChunk( encodedLength, payload );
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
