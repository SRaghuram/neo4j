/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class StoreCopyFinishedResponseEncoder extends MessageToByteEncoder<StoreCopyFinishedResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, StoreCopyFinishedResponse msg, ByteBuf out )
    {
        out.writeInt( msg.status().ordinal() );
    }
}
