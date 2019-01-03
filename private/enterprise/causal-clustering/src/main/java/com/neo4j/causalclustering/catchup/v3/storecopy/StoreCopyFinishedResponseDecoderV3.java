/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status;

public class StoreCopyFinishedResponseDecoderV3 extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out )
    {
        int statusOrdinal = msg.readInt();
        long lastCheckpointedTx = msg.readLong();
        out.add( new StoreCopyFinishedResponse( Status.values()[statusOrdinal], lastCheckpointedTx ) );
    }
}
