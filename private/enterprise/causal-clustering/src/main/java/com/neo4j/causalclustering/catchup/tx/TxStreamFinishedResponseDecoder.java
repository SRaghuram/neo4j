/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class TxStreamFinishedResponseDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out )
    {
        int ordinal = msg.readInt();
        long latestTxid = msg.readLong();
        CatchupResult status = CatchupResult.values()[ordinal];
        out.add( new TxStreamFinishedResponse( status, latestTxid ) );
    }
}
