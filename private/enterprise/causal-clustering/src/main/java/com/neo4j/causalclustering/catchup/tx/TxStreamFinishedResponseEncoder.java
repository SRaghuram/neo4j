/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class TxStreamFinishedResponseEncoder extends MessageToByteEncoder<TxStreamFinishedResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, TxStreamFinishedResponse response, ByteBuf out )
    {
        out.writeInt( response.status().ordinal() );
        out.writeLong( response.lastTxId() );
    }
}
