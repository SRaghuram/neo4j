/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class CoreSnapshotRequestEncoder extends MessageToByteEncoder<CoreSnapshotRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, CoreSnapshotRequest msg, ByteBuf out )
    {
        out.writeByte( 0 );
    }
}
