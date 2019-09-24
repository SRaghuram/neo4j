/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import static org.neo4j.causalclustering.catchup.storecopy.FileChunk.HEADER_SIZE;

public class FileChunkEncoder extends MessageToMessageEncoder<FileChunk>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, FileChunk msg, List<Object> out )
    {
        ByteBuf header = ctx.alloc().ioBuffer( HEADER_SIZE );
        header.writeInt( msg.encodedLength() );

        ByteBuf payload = msg.payload();

        CompositeByteBuf composite = ctx.alloc().compositeBuffer( 2 );

        composite.addComponent( true, header );
        composite.addComponent( true, payload );

        out.add( composite );
    }
}
