/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.NetworkWritableChannel;

public class FileChunkEncoder extends MessageToByteEncoder<FileChunk>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, FileChunk chunk, ByteBuf out ) throws Exception
    {
        FileChunk.marshal().marshal( chunk, new NetworkWritableChannel( out ) );
    }
}
